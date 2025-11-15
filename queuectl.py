#!/usr/bin/env python3

import os
import sys
import json
import sqlite3
import uuid
import time
import shlex
import signal
import subprocess
from datetime import datetime, timezone
from multiprocessing import Process
import threading

import click


DATA_DIR = os.path.expanduser("~/.queuectl")
DB_PATH = os.path.join(DATA_DIR, "queue.db")
PIDFILE = os.path.join(DATA_DIR, "workers.pids")
DEFAULT_BACKOFF_BASE = 2
DEFAULT_MAX_RETRIES = 3
WORKER_POLL_INTERVAL = 1 


os.makedirs(DATA_DIR, exist_ok=True)

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_error TEXT,
        worker_id TEXT
    );
    CREATE TABLE IF NOT EXISTS dlq (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        max_retries INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        failed_at TEXT NOT NULL,
        last_error TEXT
    );
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('backoff_base', ?)", (str(DEFAULT_BACKOFF_BASE),))
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES('max_retries', ?)", (str(DEFAULT_MAX_RETRIES),))
    conn.commit()
    conn.close()

def now_ts():
    return datetime.now(timezone.utc).isoformat()

def get_config(key, default=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row["value"]
    return default

def set_config(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config(key, value) VALUES(?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def enqueue_job(job_json_str):
    try:
        job = json.loads(job_json_str)
    except Exception as e:
        raise click.BadParameter(f"Invalid JSON: {e}")

    jid = job.get("id", str(uuid.uuid4()))
    command = job.get("command")
    if not command:
        raise click.BadParameter("Job must have a `command` field")

    max_retries = job.get("max_retries")
    if max_retries is None:
        max_retries = int(get_config("max_retries", DEFAULT_MAX_RETRIES))
    created = now_ts()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES (?, ?, 'pending', 0, ?, ?, ?)
    """, (jid, command, int(max_retries), created, created))
    conn.commit()
    conn.close()
    click.echo(f"Enqueued job {jid}")

def list_jobs(state=None, limit=100):
    conn = get_conn()
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at LIMIT ?", (state, limit))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def move_to_dlq(job_row, last_error):
    conn = get_conn()
    cur = conn.cursor()
    failed_at = now_ts()
    cur.execute("""
        INSERT OR REPLACE INTO dlq(id, command, attempts, max_retries, created_at, failed_at, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (job_row["id"], job_row["command"], job_row["attempts"], job_row["max_retries"], job_row["created_at"], failed_at, last_error))
    cur.execute("DELETE FROM jobs WHERE id=?", (job_row["id"],))
    conn.commit()
    conn.close()

def retry_from_dlq(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dlq WHERE id=?", (job_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise click.ClickException("DLQ job not found")
    now = now_ts()
    cur.execute("""
        INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at)
        VALUES (?, ?, 'pending', 0, ?, ?, ?)
    """, (row["id"], row["command"], row["max_retries"], row["created_at"], now))
    cur.execute("DELETE FROM dlq WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    click.echo(f"Retried DLQ job {job_id}")

def claim_next_job(conn, worker_id):
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute("SELECT id, command, attempts, max_retries, created_at FROM jobs WHERE state='pending' ORDER BY created_at LIMIT 1")
    row = cur.fetchone()
    if not row:
        conn.commit()
        return None
    jid = row["id"]
    cur.execute("""
        UPDATE jobs SET state='processing', worker_id=?, attempts=attempts+1, updated_at=?
        WHERE id=? AND state='pending'
    """, (worker_id, now_ts(), jid))
    if cur.rowcount == 1:
        cur.execute("SELECT * FROM jobs WHERE id=?", (jid,))
        job = cur.fetchone()
        conn.commit()
        return job
    else:
        conn.commit()
        return None

def run_command(command, timeout=None):
    """
    Run the given command string using the platform's shell:
      - Windows: cmd /c <command>
      - POSIX : /bin/bash -c "<command>" (falls back to sh if bash missing)
    Returns: (returncode:int, stdout:str, stderr:str)
    """
    try:
        if os.name == "nt":
            args = ["cmd", "/c", command]
            completed = subprocess.run(args, capture_output=True, timeout=timeout, shell=False)
        else:
            shell_path = "/bin/bash" if os.path.exists("/bin/bash") else "/bin/sh"
            args = [shell_path, "-c", command]
            completed = subprocess.run(args, capture_output=True, timeout=timeout)
        stdout = completed.stdout.decode(errors="ignore") if completed.stdout else ""
        stderr = completed.stderr.decode(errors="ignore") if completed.stderr else ""
        return completed.returncode, stdout, stderr
    except FileNotFoundError as e:
        return 127, "", str(e)
    except subprocess.TimeoutExpired as e:
        return 124, "", f"timeout: {e}"
    except Exception as e:
        return 1, "", str(e)

def worker_loop(worker_id, stop_event):
    conn = get_conn()
    backoff_base = float(get_config("backoff_base", DEFAULT_BACKOFF_BASE))
    click.echo(f"[worker {worker_id}] started (backoff_base={backoff_base})")
    while not stop_event.is_set():
        try:
            job = claim_next_job(conn, worker_id)
            if not job:
                time.sleep(WORKER_POLL_INTERVAL)
                continue
            jid = job["id"]
            click.echo(f"[worker {worker_id}] claimed job {jid} - running: {job['command']}")
            ret, out, err = run_command(job["command"])
            if ret == 0:
                cur = conn.cursor()
                cur.execute("UPDATE jobs SET state='completed', updated_at=?, worker_id=NULL WHERE id=?", (now_ts(), jid))
                conn.commit()
                click.echo(f"[worker {worker_id}] job {jid} completed")
            else:
                msg = f"exit={ret}; stderr={err.strip()[:400]}"
                cur = conn.cursor()
                cur.execute("SELECT attempts, max_retries FROM jobs WHERE id=?", (jid,))
                fresh = cur.fetchone()
                if not fresh:
                    conn.commit()
                    continue
                attempts = fresh["attempts"]
                max_retries = fresh["max_retries"]
                if attempts > max_retries:
                    click.echo(f"[worker {worker_id}] job {jid} exceeded max_retries ({attempts}>{max_retries}) -> DLQ")
                    cur.execute("SELECT * FROM jobs WHERE id=?", (jid,))
                    jobrow = cur.fetchone()
                    move_to_dlq(jobrow, msg)
                else:
                    delay = float(backoff_base) ** attempts
                    click.echo(f"[worker {worker_id}] job {jid} failed (attempt {attempts}/{max_retries}). will retry after {delay:.1f}s. err:{msg}")
                    cur.execute("UPDATE jobs SET state='pending', updated_at=?, worker_id=NULL, last_error=? WHERE id=?", (now_ts(), msg, jid))
                    conn.commit()
                    time.sleep(delay)
        except Exception as e:
            click.echo(f"[worker {worker_id}] ERROR: {e}", err=True)
            time.sleep(1)
    conn.close()
    click.echo(f"[worker {worker_id}] shutting down gracefully")

@click.group()
def cli():
    init_db()

@cli.command(help="Enqueue a job JSON string. Example: queuectl enqueue '{\"id\":\"job1\",\"command\":\"sleep 2\"}'")
@click.argument("job_json", nargs=1)
def enqueue(job_json):
    enqueue_job(job_json)

@cli.group(help="Worker operations")
def worker():
    pass

@worker.command("start", help="Start N workers (keeps running in foreground). Example: queuectl worker start --count 3")
@click.option("--count", "-c", default=1, help="Number of worker processes to start")
@click.option("--detach/--no-detach", default=False, help="If --detach, tries to spawn worker processes detached (unix only). If false, runs in foreground.")
def worker_start(count, detach):
    if detach:
        pids = []
        for i in range(count):
            worker_id = f"w-{int(time.time())}-{i}"
            args = [sys.executable, os.path.realpath(__file__), "_run_worker", "--worker-id", worker_id]
            try:
                proc = subprocess.Popen(args, stdout=open(os.path.join(DATA_DIR, f"worker-{worker_id}.log"), "a"),
                                        stderr=subprocess.STDOUT, preexec_fn=os.setsid)
            except TypeError:
                proc = subprocess.Popen(args, stdout=open(os.path.join(DATA_DIR, f"worker-{worker_id}.log"), "a"),
                                        stderr=subprocess.STDOUT)
            pids.append(proc.pid)
            click.echo(f"Spawned detached worker {worker_id} pid={proc.pid}")
        with open(PIDFILE, "w") as f:
            f.write("\n".join(map(str, pids)))
        click.echo(f"Spawned {count} detached workers (pids written to {PIDFILE}). Logs in {DATA_DIR}")
        return

    stop_event = threading.Event()

    def handle_sigint(sig, frame):
        click.echo("Received stop signal, shutting down workers gracefully...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    procs = []
    for i in range(count):
        worker_id = f"fg-{os.getpid()}-{i}"
        p = Process(target=_foreground_worker_entry, args=(worker_id,))
        p.start()
        procs.append(p)
        click.echo(f"Started foreground worker {worker_id} pid={p.pid}")
    click.echo("Workers running in foreground. Press Ctrl-C to stop.")
    try:
        while any(p.is_alive() for p in procs):
            time.sleep(0.5)
            if stop_event.is_set():
                for p in procs:
                    try:
                        os.kill(p.pid, signal.SIGTERM)
                    except Exception:
                        pass
                break
    except KeyboardInterrupt:
        for p in procs:
            try:
                os.kill(p.pid, signal.SIGTERM)
            except Exception:
                pass
    for p in procs:
        p.join()
    click.echo("All workers stopped")

def _foreground_worker_entry(worker_id):
    stopped = {"stop": False}
    def _handle(sig, frame):
        stopped["stop"] = True
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    class SimpleStop:
        def is_set(self):
            return stopped["stop"]
    worker_loop(worker_id, SimpleStop())

@cli.command(help="Stop workers started in detached mode. Reads pidfile.")
def stop():
    if not os.path.exists(PIDFILE):
        click.echo("No pidfile found. Are any detached workers running?")
        return
    with open(PIDFILE) as f:
        pids = [int(line.strip()) for line in f if line.strip()]
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            click.echo(f"Sent SIGTERM to pid {pid}")
        except ProcessLookupError:
            click.echo(f"Process {pid} not found")
        except Exception as e:
            click.echo(f"Failed to stop {pid}: {e}")
    try:
        os.remove(PIDFILE)
    except Exception:
        pass
    click.echo("Stop signals sent to workers.")

@cli.command(help="Show overall status (counts by state and active workers)")
def status():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
    rows = cur.fetchall()
    click.echo("Jobs by state:")
    for r in rows:
        click.echo(f"  {r['state']}: {r['cnt']}")
    cur.execute("SELECT COUNT(*) as c FROM dlq")
    dlq_count = cur.fetchone()["c"]
    click.echo(f"DLQ: {dlq_count}")
    active = []
    if os.path.exists(PIDFILE):
        with open(PIDFILE) as f:
            pids = [int(line.strip()) for line in f if line.strip()]
        for pid in pids:
            try:
                os.kill(pid, 0)
                active.append(pid)
            except Exception:
                pass
    click.echo(f"Active detached workers: {len(active)}")
    conn.close()

@cli.command(help="List jobs. Example: queuectl list --state pending")
@click.option("--state", "-s", default=None, help="Filter by job state (pending, processing, completed, failed)")
def list(state):
    rows = list_jobs(state)
    if not rows:
        click.echo("No jobs")
        return
    for r in rows:
        click.echo(json.dumps(r, default=str))

@cli.group(help="DLQ operations")
def dlq():
    pass

@dlq.command("list", help="List dead-lettered jobs")
def dlq_list():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM dlq ORDER BY failed_at DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for r in rows:
        click.echo(json.dumps(r, default=str))

@dlq.command("retry", help="Retry a DLQ job: queuectl dlq retry <job_id>")
@click.argument("job_id", nargs=1)
def dlq_retry(job_id):
    retry_from_dlq(job_id)

@cli.group(help="Configuration")
def config():
    pass

@config.command("set", help="Set configuration key. e.g. queuectl config set backoff_base 3")
@click.argument("key", nargs=1)
@click.argument("value", nargs=1)
def config_set(key, value):
    set_config(key, value)
    click.echo(f"Set {key} = {value}")

@config.command("get", help="Get config value")
@click.argument("key", nargs=1)
def config_get(key):
    val = get_config(key)
    if val is None:
        click.echo("Not set")
    else:
        click.echo(val)

@cli.command(hidden=True)
@click.option("--worker-id", required=True)
def _run_worker(worker_id):
    stop_flag = {"stop": False}
    def _handler(sig, frame):
        stop_flag["stop"] = True
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    conn = get_conn()
    backoff_base = float(get_config("backoff_base", DEFAULT_BACKOFF_BASE))
    click.echo(f"[detached-worker {worker_id}] started (backoff_base={backoff_base})")
    while not stop_flag["stop"]:
        try:
            job = claim_next_job(conn, worker_id)
            if not job:
                time.sleep(WORKER_POLL_INTERVAL)
                continue
            jid = job["id"]
            click.echo(f"[detached-worker {worker_id}] running job {jid}: {job['command']}")
            ret, out, err = run_command(job["command"])
            if ret == 0:
                cur = conn.cursor()
                cur.execute("UPDATE jobs SET state='completed', updated_at=?, worker_id=NULL WHERE id=?", (now_ts(), jid))
                conn.commit()
                click.echo(f"[detached-worker {worker_id}] job {jid} completed")
            else:
                msg = f"exit={ret}; err={err.strip()[:400]}"
                cur = conn.cursor()
                cur.execute("SELECT attempts, max_retries FROM jobs WHERE id=?", (jid,))
                fresh = cur.fetchone()
                if not fresh:
                    continue
                attempts = fresh["attempts"]
                max_retries = fresh["max_retries"]
                if attempts > max_retries:
                    cur.execute("SELECT * FROM jobs WHERE id=?", (jid,))
                    jobrow = cur.fetchone()
                    move_to_dlq(jobrow, msg)
                    click.echo(f"[detached-worker {worker_id}] job {jid} moved to DLQ")
                else:
                    delay = float(backoff_base) ** attempts
                    cur.execute("UPDATE jobs SET state='pending', updated_at=?, worker_id=NULL, last_error=? WHERE id=?", (now_ts(), msg, jid))
                    conn.commit()
                    click.echo(f"[detached-worker {worker_id}] job {jid} failed; retrying after {delay:.1f}s (attempt {attempts}/{max_retries})")
                    time.sleep(delay)
        except Exception as e:
            click.echo(f"[detached-worker {worker_id}] ERROR: {e}", err=True)
            time.sleep(1)
    conn.close()
    click.echo(f"[detached-worker {worker_id}] exiting gracefully")


@cli.command("demo", help="Quick demo: enqueue 3 jobs and start a worker (foreground 1).")
def demo():
    if os.name == "nt":
        enqueue_job(json.dumps({"id": "demo-1", "command": "echo hello world", "max_retries": 2}))
        enqueue_job(json.dumps({"id": "demo-2", "command": "cmd /c \"exit 1\"", "max_retries": 2}))
        enqueue_job(json.dumps({"id": "demo-3", "command": "timeout /t 2 > nul & echo done", "max_retries": 2}))
    else:
        enqueue_job(json.dumps({"id": "demo-1", "command": "echo hello world", "max_retries": 2}))
        enqueue_job(json.dumps({"id": "demo-2", "command": "bash -c 'exit 1'", "max_retries": 2}))
        enqueue_job(json.dumps({"id": "demo-3", "command": "sleep 2 && echo done", "max_retries": 2}))
    click.echo("Starting 1 worker in foreground for demo. Ctrl-C to stop.")
    ctx = click.get_current_context()
    ctx.invoke(worker_start, count=1, detach=False)

if __name__ == "__main__":
    cli()

