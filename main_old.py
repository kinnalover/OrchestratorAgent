import asyncio
import threading
import time
import json
from websocket import WebSocketApp
from fastapi import FastAPI
from process import Process
from concurrent.futures import ThreadPoolExecutor
import socket

executor = ThreadPoolExecutor(max_workers=1)
app = FastAPI()

ip_address = socket.gethostbyname(socket.gethostname())
ORCHESTRATOR_WS_URL = f"ws://localhost:5000/ws/{ip_address}"

# Thread-safe queue for sending messages from worker threads to WebSocket thread
message_queue = asyncio.Queue()
event_loop = asyncio.get_event_loop()


class Agent:
    def __init__(self, machine_id):
        self.machine_id = machine_id
        self.ws = None
        self.is_running_script = False

    def on_open(self, ws):
        """Called when the WebSocket connection is opened."""
        print(f"Connected to orchestrator as {self.machine_id}")
        asyncio.run_coroutine_threadsafe(self.send_heartbeat(), event_loop)

    def on_message(self, ws, message):
        """Called when a message is received from the orchestrator."""
        if "You wrote:" not in message and "Client " not in message:
            print(f"Received message from orchestrator: {message}")
            asyncio.run_coroutine_threadsafe(self.handle_message(message), event_loop)

    def on_close(self, ws, close_status_code, close_msg):
        """Called when the WebSocket connection is closed."""
        print(f"Connection closed. Status code: {close_status_code}, Close message: {close_msg}")
        print("Retrying connection in 5 seconds...")
        asyncio.run_coroutine_threadsafe(self.connect_to_orchestrator(), event_loop)

    def on_error(self, ws, error):
        """Called when an error occurs."""
        print(f"Error occurred: {error}")
        print("Retrying connection in 5 seconds...")
        asyncio.run_coroutine_threadsafe(self.connect_to_orchestrator(), event_loop)

    def run_script(self, gitlab_repo):
        """Run the external script."""
        robot = Process(gitlab_repo)
        stdout, stderr = robot.run_script()
        return stdout, stderr

    async def execute_script_task(self, gitlab_repo, job_id):
        """Run the script in a separate thread, but send results from the asyncio loop."""
        loop = asyncio.get_event_loop()
        # Run the blocking script in a separate thread
        stdout, stderr = await loop.run_in_executor(executor, self.run_script, gitlab_repo)

        # After the thread completes, send the results via the queue
        status = "success" if not stderr else "failed"
        result_message = json.dumps({
            "type": "process_result",
            "machine_id": self.machine_id,
            "stdout": stdout,
            "stderr": stderr,
            "job_id": job_id,
            "status": status
        })

        print(f"Process results: {result_message}")
        await message_queue.put(result_message)  # Add the result message to the queue
        self.is_running_script = False

    async def send_heartbeat(self):
        """Send heartbeats to the orchestrator at regular intervals."""
        while True:
            heartbeat_message = json.dumps({
                "type": "heartbeat",
                "machine_id": self.machine_id,
                "status": "online",
                "timestamp": int(time.time())
            })
            print("heart")
            await message_queue.put(heartbeat_message)  # Add the heartbeat to the queue
            await asyncio.sleep(5)  # Send heartbeat every 5 seconds

    async def handle_message(self, message):
        """Process the incoming message from the orchestrator."""
        data = json.loads(message)
        if data.get('type') == 'run_process':
            gitlab_repo = data.get('gitlab_repo')
            job_id = data.get('job_id')
            print("Received task to run the process")

            if not self.is_running_script:
                # Notify orchestrator that the process has started
                result_message = json.dumps({
                    "type": "process_result",
                    "machine_id": self.machine_id,
                    "stdout": None,
                    "stderr": None,
                    "job_id": job_id,
                    "status": "running"
                })
                await message_queue.put(result_message)

                # Mark the agent as busy
                self.is_running_script = True
                # Run the task asynchronously without blocking WebSocket
                await self.execute_script_task(gitlab_repo=gitlab_repo, job_id=job_id)
            else:
                print("Already running a task")
                response = json.dumps({
                    "type": "wait",
                    "answer": "already running a process"
                })
                await message_queue.put(response)

    async def connect_to_orchestrator(self):
        """Initialize WebSocket connection to the orchestrator."""
        self.ws = WebSocketApp(
            ORCHESTRATOR_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_close=self.on_close,
            on_error=self.on_error
        )

        # Use asyncio to run WebSocket in the event loop
        await asyncio.get_event_loop().run_in_executor(None, self.ws.run_forever)

    async def process_message_queue(self):
        """Continuously check the message queue and send messages through WebSocket."""
        while True:
            message = await message_queue.get()
            if self.ws and self.ws.sock and self.ws.sock.connected:
                try:
                    self.ws.send(message)
                    print(f"Sent message: {message}")
                except Exception as ex:
                    print(f"Error sending message: {ex}")
            else:
                print("WebSocket not connected, unable to send message.")
            message_queue.task_done()
            await asyncio.sleep(0)  # Yield control to the event loop


# Initialize the agent with a unique machine ID
agent = Agent(machine_id=ip_address)


@app.on_event("startup")
async def start_agent():
    """Start the agent WebSocket connection and message processing on application startup."""
    asyncio.create_task(agent.connect_to_orchestrator())  # Start WebSocket connection
    asyncio.create_task(agent.process_message_queue())  # Start message processing


@app.get("/")
def read_root():
    """Basic route to ensure the agent is running."""
    return {"message": "Agent is running"}


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001)
