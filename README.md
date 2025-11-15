# Flam-Assignment
# queuectl — A Minimal Background Job Queue with Workers, Retries & DLQ

`queuectl` is a CLI-driven background job processing system that supports:

- Persistent job storage  
- Parallel worker processes  
- Automatic retries with exponential backoff  
- Dead Letter Queue (DLQ) for permanently failed jobs  
- Configurable retry and backoff settings  
- A clean, production-style command-line interface  

This project implements a simplified internal job queue similar to Celery, Sidekiq, and Resque — but entirely in Python and with zero external dependencies beyond SQLite.

---

# 🚀 **1. Setup Instructions**

### **Prerequisites**
- Python **3.9+**
- Works on **Windows**, **Linux**, and **macOS**
- No external services required (uses SQLite for persistence)

### **Install Dependencies**
```bash
pip install click
```

### **Clone Repository**
```bash
git clone <your-repo-url>
cd queuectl
```

### **Run CLI**
All commands use:

```bash
python queuectl.py <command> <options>
```

---

# 📌 **2. Usage Examples**

Below are the commands with real, working examples.

---

## **Enqueue a Job**

### PowerShell (recommended):

```powershell
python queuectl.py enqueue '{"id":"job1","command":"echo hello","max_retries":2}'
```

### CMD alternative:
```cmd
python queuectl.py enqueue "{\"id\":\"job1\",\"command\":\"echo hello\",\"max_retries\":2}"
```

---

## **Start Workers**

### Start 1 worker (foreground):

```bash
python queuectl.py worker start --count 1
```

### Start 3 workers:

```bash
python queuectl.py worker start --count 3
```

---

## **List Jobs**

```bash
python queuectl.py list --state pending
```

---

## **View Status**
```bash
python queuectl.py status
```

---

## **Dead Letter Queue (DLQ)**

```bash
python queuectl.py dlq list
python queuectl.py dlq retry job1
```

---

## **Configuration Management**

```bash
python queuectl.py config set backoff_base 3
python queuectl.py config get backoff_base
```

---

## **Demo Mode**

```bash
python queuectl.py demo
```

---

# 🧱 **3. Architecture Overview**

## **Job Model**

Each job includes:
```json
{
  "id": "unique-job-id",
  "command": "echo hello",
  "state": "pending | processing | completed | failed",
  "attempts": 0,
  "max_retries": 3
}
```

## **Lifecycle**
- pending → processing → completed  
- pending → processing → retry → DLQ  

## **Workers**
- Each worker runs in a separate process.
- Atomic job claiming using SQLite locking.
- Performs exponential backoff retries.

## **Data Persistence**
- Stored at `~/.queuectl/queue.db`
- WAL mode enabled for concurrency
- Jobs survive restarts

---

# ⚖️ **4. Assumptions & Trade-offs**

- SQLite for simplicity  
- Shell execution chosen for cross-platform support  
- No job priority (FIFO)  
- Exponential backoff only  
- Single-file implementation for easier review  

---

# 🧪 **5. Testing Instructions**

## **A. Basic Success**

```bash
python queuectl.py enqueue '{"id":"ok","command":"echo hello"}'
python queuectl.py worker start --count 1
```

## **B. Failing Job → DLQ**

```bash
python queuectl.py enqueue '{"id":"bad","command":"cmd /c \"exit 1\""}'
```

## **C. Retry DLQ Job**

```bash
python queuectl.py dlq retry bad
```

## **D. Persistence Test**

```bash
python queuectl.py enqueue '{"id":"p1","command":"echo persist"}'
python queuectl.py list
```

---

