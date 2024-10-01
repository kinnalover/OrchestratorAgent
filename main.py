import asyncio
import uvicorn
import websockets
import json
from fastapi import FastAPI, WebSocket
import socket
import time
from process import Process, logger
app = FastAPI()

# Get the agent's IP address
ip_address = socket.gethostbyname(socket.gethostname())

# Connection details for the orchestrator (websocket)
ORCHESTRATOR_URL = f"ws://localhost:5000/ws/{ip_address}"
heartbeat_interval = 5  # Send heartbeats every 5 seconds

# Global variable to track WebSocket connection with orchestrator
orchestrator_websocket = None

# Tasks to manage heartbeat and command listener
heartbeat_task = None
command_listener_task = None


async def connect_to_orchestrator():
    global orchestrator_websocket, heartbeat_task, command_listener_task
    while True:
        try:
            # Cancel existing tasks if they are still running
            if heartbeat_task and not heartbeat_task.cancelled():
                heartbeat_task.cancel()
            if command_listener_task and not command_listener_task.cancelled():
                command_listener_task.cancel()

            # Establish WebSocket connection with orchestrator
            orchestrator_websocket = await websockets.connect(ORCHESTRATOR_URL, ping_interval=20, ping_timeout=30)
            print("Connected to orchestrator.")

            # Start sending heartbeats and listening for commands
            heartbeat_task = asyncio.create_task(send_heartbeat())
            command_listener_task = asyncio.create_task(listen_to_commands())

            # Wait for the tasks to finish (if they ever break, they reconnect)
            await asyncio.gather(heartbeat_task, command_listener_task)
        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK, ConnectionRefusedError) as e:
            print(f"WebSocket connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Wait before reconnecting
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)  # Wait before reconnecting


async def send_heartbeat():
    """Send heartbeats to orchestrator every few seconds."""
    global orchestrator_websocket
    while orchestrator_websocket:
        try:
            heartbeat_message = json.dumps({
                "type": "heartbeat",
                "machine_id": ip_address,
                "status": "online",
                "timestamp": int(time.time())
            })
            await orchestrator_websocket.send(heartbeat_message)
            print("Sent heartbeat")
        except websockets.ConnectionClosed:
            print("WebSocket connection closed during heartbeat.")
            break  # Break to allow reconnection
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
            break  # Break to allow reconnection
        await asyncio.sleep(heartbeat_interval)


async def listen_to_commands():
    """Listen for commands from the orchestrator."""
    global orchestrator_websocket
    try:
        while orchestrator_websocket:
            try:
                message = await asyncio.wait_for(orchestrator_websocket.recv(), timeout=30)
                if "You wrote:" not in message and "Client " not in message:
                    print(f"Received message from orchestrator: {message}")
                    data = json.loads(message)
                    if data.get("type") == "run_process":
                        gitlab_repo = data.get("gitlab_repo")
                        job_id = data.get("job_id")
                        print(f"Received task to run the process for job_id: {job_id}, repo: {gitlab_repo}")
                        # Run the task in the background
                        asyncio.create_task(run_long_running_task(gitlab_repo, job_id))
            except asyncio.TimeoutError:
                print("Timed out waiting for commands.")
            except websockets.ConnectionClosedError as err:
                print(f"WebSocket connection closed while listening to commands. {err}: {err.reason}")
                break  # Break to allow reconnection
    except Exception as e:
        print(f"Error receiving message: {e}")

def run_script( gitlab_repo):
    """Run the external script."""
    robot = Process(gitlab_repo)
    stdout, stderr = robot.run_script()
    return stdout, stderr

async def run_long_running_task(gitlab_repo: str, job_id: str):
    """Simulate running a long-running task."""
    try:
        result_message = json.dumps({
            "type": "process_result",
            "machine_id": ip_address,
            "stdout": None,
            "stderr": None,
            "job_id": job_id,
            "status": "running"
        })
        await orchestrator_websocket.send(result_message)
        print(f"Sent task result for job_id: {job_id}")
    except websockets.ConnectionClosed:
        print("WebSocket connection closed while sending task result.")
    print(f"Running long-running task for job_id: {job_id}, repo: {gitlab_repo}")
    await asyncio.to_thread(run_script, gitlab_repo) # Simulate long task with sleep
    result_m = f"Completed task for job_id: {job_id}"
    print(result_m)
    logger.info(result_m)
    # Send result back to orchestrator
    if orchestrator_websocket:
        result_message = json.dumps({"type": "process_result", "job_id": job_id, "status": "completed"})
        try:
            await orchestrator_websocket.send(result_message)
            print(f"Sent task result for job_id: {job_id}")
        except websockets.ConnectionClosed:
            print("WebSocket connection closed while sending task result.")



@app.on_event("startup")
async def startup_event():
    """Run when FastAPI app starts up."""
    asyncio.create_task(connect_to_orchestrator())  # Connect to orchestrator when the app starts


@app.on_event("shutdown")
async def shutdown_event():
    """Handle shutdown gracefully, closing the WebSocket connection."""
    global orchestrator_websocket
    if orchestrator_websocket:
        await orchestrator_websocket.close()
    print("Shutting down, WebSocket connection closed.")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for the agent."""
    await websocket.accept()
    await websocket.send_text("WebSocket connection established.")
    while True:
        data = await websocket.receive_text()
        print(f"Received message from WebSocket client: {data}")
        await websocket.send_text(f"Echo: {data}")


if __name__ == '__main__':
    uvicorn.run(app, host="127.0.0.1", port=8001)
