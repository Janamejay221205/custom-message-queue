from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from collections import deque
import json
import os
import uuid

app = FastAPI(title="Production-Grade Message Queue")

message_queue = deque()
# A dictionary to keep track of messages currently being worked on
processing_pool = {}
LOG_FILE = "queue_storage.txt"

class MessagePayload(BaseModel):
    body: str

def save_all_to_disk():
    """Helper to sync current states back to our hard drive file"""
    with open(LOG_FILE, "w") as f:
        # Save messages still waiting in queue
        for msg in message_queue:
            f.write(json.dumps(msg) + "\n")
        # Save messages currently being processed by workers
        for msg in processing_pool.values():
            f.write(json.dumps(msg) + "\n")

def rebuild_queue_from_disk():
    if not os.path.exists(LOG_FILE):
        return
    print("🔄 Recovering database from disk...")
    with open(LOG_FILE, "r") as f:
        for line in f:
            if line.strip():
                msg = json.loads(line.strip())
                # If a message was left 'PROCESSING' when the server crashed, 
                # we safely put it back to 'AVAILABLE' so a worker can try again!
                if msg["status"] in ["AVAILABLE", "PROCESSING"]:
                    msg["status"] = "AVAILABLE"
                    message_queue.append(msg)

rebuild_queue_from_disk()

@app.post("/publish")
def publish_message(payload: MessagePayload):
    message_id = str(uuid.uuid4())
    formatted_message = {"id": message_id, "body": payload.body, "status": "AVAILABLE"}
    
    message_queue.append(formatted_message)
    save_all_to_disk()
    return {"status": "Success", "message_id": message_id, "queue_size": len(message_queue)}

@app.get("/consume")
def consume_message():
    if not message_queue:
        raise HTTPException(status_code=404, detail="No messages available.")
    
    # 1. Take message out of the main waiting pool
    message = message_queue.popleft()
    
    # 2. Change status to PROCESSING so no other worker can fetch it
    message["status"] = "PROCESSING"
    
    # 3. Move it to the active processing pool temporary holding lock
    processing_pool[message["id"]] = message
    
    # 4. Update our hard drive log
    save_all_to_disk()
    
    return {"status": "Locked for processing", "data": message}

@app.post("/ack/{message_id}")
def acknowledge_message(message_id: str):
    """
    Workers call this when they finish their job successfully.
    This safely deletes the message out of the system forever.
    """
    if message_id not in processing_pool:
        raise HTTPException(status_code=404, detail="Message ID not found in active processing pool.")
    
    # Remove from processing memory map
    del processing_pool[message_id]
    
    # Update hard drive file to reflect deletion
    save_all_to_disk()
    return {"status": "Confirmed", "detail": f"Message {message_id} completed and cleared."}