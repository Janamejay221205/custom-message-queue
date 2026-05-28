# Custom Fault-Tolerant & Durable Message Queue

A lightweight, production-grade asynchronous message broker built from scratch in Python using **FastAPI**. This system decouples data producers from data workers and features crash recovery mechanisms and concurrency controls to prevent duplicate message processing.

---

## 🏗️ System Architecture

The architecture consists of three main stages ensuring data integrity and concurrent worker safety:

1. **Publish (`POST /publish`)**: Receives data payloads, assigns a unique UUID, persists the state to disk, and pushes it to an in-memory deque.
2. **Consume (`GET /consume`)**: Implements strict FIFO ordering. Transitions message states from `AVAILABLE` to `PROCESSING`, locking them from concurrent workers.
3. **Acknowledge (`POST /ack/{message_id}`)**: Confirms successful worker execution and safely purges the completed message from the system.

---

## ⚡ Key Engineering Features

* **Data Durability (Fault Tolerance):** Implements a state synchronization layer. Every transaction is appended to a local storage file (`queue_storage.txt`) before network acknowledgment. If the server crashes or restarts, it automatically reconstructs the waiting queue back into memory with zero data loss.
* **Concurrency Control (Worker Safety):** Protects against duplicate processing in multi-worker environments. When a message is fetched, it is placed in an isolated processing pool. If a worker fails, the message remains safely tracked for retries rather than vanishing.
* **Optimized Memory Operations:** Utilizes Python's `collections.deque` for fast O(1) head-popping performance, ensuring the broker remains highly responsive under continuous load.

---

## 🚀 How to Run and Test

### 1. Installation
Clone the repository and install the required dependencies:
```bash
pip install fastapi uvicorn
