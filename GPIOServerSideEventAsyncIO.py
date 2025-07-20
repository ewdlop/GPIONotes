#!/usr/bin/env python3
"""
Async SSE Server for sending GPIO commands to Raspberry Pi using FastAPI
"""

import json
import asyncio
import uuid
from datetime import datetime
from typing import Dict, Any, Set
import logging

try:
    from fastapi import FastAPI, Request, HTTPException
    from fastapi.responses import StreamingResponse, HTMLResponse
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    print("Please install FastAPI and dependencies: pip install fastapi uvicorn pydantic")
    import sys
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="GPIO SSE Command Server", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state for connected clients
connected_clients: Set[asyncio.Queue] = set()
command_counter = 0

class GPIOCommand(BaseModel):
    type: str
    pin: int = None
    state: bool = None

class CommandResponse(BaseModel):
    success: bool
    command_id: str
    message: str

async def add_client(queue: asyncio.Queue):
    """Add a new SSE client"""
    connected_clients.add(queue)
    logger.info(f"Client connected. Total clients: {len(connected_clients)}")

async def remove_client(queue: asyncio.Queue):
    """Remove SSE client"""
    connected_clients.discard(queue)
    logger.info(f"Client disconnected. Total clients: {len(connected_clients)}")

async def broadcast_command(command: Dict[str, Any]):
    """Broadcast command to all connected clients"""
    if not connected_clients:
        logger.warning("No clients connected to receive command")
        return
    
    # Create tasks for all clients
    tasks = []
    for client_queue in connected_clients.copy():  # Copy to avoid modification during iteration
        tasks.append(send_to_client(client_queue, command))
    
    # Send to all clients concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Log any errors
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Failed to send command to client {i}: {result}")

async def send_to_client(client_queue: asyncio.Queue, command: Dict[str, Any]):
    """Send command to a specific client"""
    try:
        await asyncio.wait_for(client_queue.put(command), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("Client queue timeout - removing slow client")
        await remove_client(client_queue)
    except Exception as e:
        logger.error(f"Error sending to client: {e}")
        await remove_client(client_queue)

@app.get("/")
async def index():
    """Main control interface"""
    return HTMLResponse(content=GPIO_CONTROL_HTML)

@app.get("/events")
async def events(request: Request):
    """SSE endpoint for GPIO commands"""
    
    async def event_generator():
        # Create client queue
        client_queue = asyncio.Queue(maxsize=100)
        await add_client(client_queue)
        
        try:
            # Send initial connection message
            initial_message = {
                'type': 'connected',
                'client_id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat()
            }
            yield f"data: {json.dumps(initial_message)}\n\n"
            
            # Send heartbeat and process commands
            heartbeat_task = asyncio.create_task(heartbeat_sender(client_queue))
            
            try:
                while True:
                    # Check if client is still connected
                    if await request.is_disconnected():
                        logger.info("Client disconnected")
                        break
                    
                    try:
                        # Wait for command with timeout
                        command = await asyncio.wait_for(client_queue.get(), timeout=1.0)
                        yield f"data: {json.dumps(command)}\n\n"
                        client_queue.task_done()
                        
                    except asyncio.TimeoutError:
                        # Send keep-alive
                        yield f": keep-alive\n\n"
                        continue
                        
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
                    
        except asyncio.CancelledError:
            logger.info("Event generator cancelled")
        except Exception as e:
            logger.error(f"Error in event generator: {e}")
        finally:
            await remove_client(client_queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control"
        }
    )

async def heartbeat_sender(client_queue: asyncio.Queue):
    """Send periodic heartbeat messages"""
    try:
        while True:
            await asyncio.sleep(30)  # Send heartbeat every 30 seconds
            heartbeat = {
                'type': 'heartbeat',
                'timestamp': datetime.now().isoformat()
            }
            await client_queue.put(heartbeat)
    except asyncio.CancelledError:
        pass

@app.post("/api/command")
async def send_command(command: GPIOCommand):
    """API endpoint for sending GPIO commands"""
    global command_counter
    
    try:
        command_counter += 1
        command_dict = {
            'id': f"cmd_{command_counter}_{uuid.uuid4().hex[:8]}",
            'type': command.type,
            'timestamp': datetime.now().isoformat()
        }
        
        # Add optional fields
        if command.pin is not None:
            command_dict['pin'] = command.pin
        if command.state is not None:
            command_dict['state'] = command.state
        
        # Broadcast to all connected clients
        await broadcast_command(command_dict)
        
        return CommandResponse(
            success=True,
            command_id=command_dict['id'],
            message='Command sent successfully'
        )
        
    except Exception as e:
        logger.error(f"Error sending command: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/status")
async def get_status():
    """Get server status"""
    return {
        'connected_clients': len(connected_clients),
        'server_time': datetime.now().isoformat(),
        'status': 'running',
        'commands_sent': command_counter
    }

@app.get("/api/test")
async def test_commands():
    """Test endpoint with example commands"""
    test_commands = [
        {'type': 'set_output', 'pin': 18, 'state': True},   # Turn on LED
        {'type': 'get_input', 'pin': 25},                   # Read button
        {'type': 'toggle_output', 'pin': 23},               # Toggle relay
        {'type': 'get_status'},                             # Get all pin status
        {'type': 'ping'}                                    # Ping test
    ]
    
    return {
        'test_commands': test_commands,
        'usage': 'POST these commands to /api/command to test the GPIO controller',
        'example': 'curl -X POST "http://localhost:8000/api/command" -H "Content-Type: application/json" -d \'{"type": "set_output", "pin": 18, "state": true}\''
    }

# Bulk command endpoint
@app.post("/api/commands/bulk")
async def send_bulk_commands(commands: list[GPIOCommand]):
    """Send multiple commands at once"""
    results = []
    
    for cmd in commands:
        try:
            result = await send_command(cmd)
            results.append({'success': True, 'command': cmd.dict(), 'result': result})
        except Exception as e:
            results.append({'success': False, 'command': cmd.dict(), 'error': str(e)})
    
    return {
        'total_commands': len(commands),
        'results': results,
        'timestamp': datetime.now().isoformat()
    }

# WebSocket endpoint for real-time bidirectional communication
@app.websocket("/ws")
async def websocket_endpoint(websocket):
    """WebSocket endpoint for real-time communication"""
    try:
        await websocket.accept()
        logger.info("WebSocket client connected")
        
        # Create client queue for this WebSocket
        client_queue = asyncio.Queue(maxsize=100)
        await add_client(client_queue)
        
        # Send initial connection message
        initial_message = {
            'type': 'ws_connected',
            'client_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat()
        }
        await websocket.send_text(json.dumps(initial_message))
        
        # Handle bidirectional communication
        async def send_messages():
            """Send messages from queue to WebSocket"""
            try:
                while True:
                    command = await client_queue.get()
                    await websocket.send_text(json.dumps(command))
                    client_queue.task_done()
            except Exception as e:
                logger.error(f"Error sending WebSocket message: {e}")
        
        async def receive_messages():
            """Receive messages from WebSocket"""
            try:
                while True:
                    data = await websocket.receive_text()
                    try:
                        message = json.loads(data)
                        logger.info(f"Received WebSocket message: {message}")
                        
                        # Echo back or process the message
                        if message.get('type') == 'ping':
                            response = {
                                'type': 'pong',
                                'timestamp': datetime.now().isoformat(),
                                'original_message': message
                            }
                            await websocket.send_text(json.dumps(response))
                        
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON received: {data}")
                        
            except Exception as e:
                logger.error(f"Error receiving WebSocket message: {e}")
        
        # Run both send and receive concurrently
        send_task = asyncio.create_task(send_messages())
        receive_task = asyncio.create_task(receive_messages())
        
        try:
            await asyncio.gather(send_task, receive_task)
        finally:
            send_task.cancel()
            receive_task.cancel()
            await remove_client(client_queue)
            
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        logger.info("WebSocket client disconnected")

# HTML template for the control interface
GPIO_CONTROL_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Async Raspberry Pi GPIO Control</title>
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: white;
        }
        .container { 
            max-width: 1000px; 
            margin: 0 auto; 
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            padding: 30px; 
            border-radius: 15px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }
        .status { 
            padding: 15px; 
            margin: 10px 0; 
            border-radius: 10px; 
            font-weight: bold;
        }
        .status.connected { 
            background: rgba(39, 174, 96, 0.2); 
            color: #2ecc71; 
            border: 1px solid #27ae60; 
        }
        .status.disconnected { 
            background: rgba(231, 76, 60, 0.2); 
            color: #e74c3c; 
            border: 1px solid #c0392b; 
        }
        .pin-group { 
            margin: 20px 0; 
            padding: 20px; 
            border: 1px solid rgba(255, 255, 255, 0.2); 
            border-radius: 10px; 
            background: rgba(255, 255, 255, 0.05);
        }
        .pin-group h3 { 
            margin-top: 0; 
            color: #ffd700; 
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
        }
        button { 
            padding: 12px 20px; 
            margin: 8px; 
            border: none; 
            border-radius: 8px; 
            cursor: pointer; 
            font-size: 14px; 
            font-weight: bold;
            transition: all 0.3s ease;
        }
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.3);
        }
        .btn-on { background: linear-gradient(45deg, #28a745, #20c997); color: white; }
        .btn-off { background: linear-gradient(45deg, #dc3545, #fd7e14); color: white; }
        .btn-toggle { background: linear-gradient(45deg, #17a2b8, #6f42c1); color: white; }
        .btn-read { background: linear-gradient(45deg, #6c757d, #495057); color: white; }
        .btn-bulk { background: linear-gradient(45fd, #ffc107, #fd7e14); color: #212529; }
        .log { 
            max-height: 300px; 
            overflow-y: auto; 
            background: rgba(0, 0, 0, 0.3); 
            padding: 15px; 
            border-radius: 8px; 
            font-family: 'Courier New', monospace; 
            font-size: 12px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        input[type="number"] { 
            width: 80px; 
            padding: 8px; 
            border: none; 
            border-radius: 5px; 
            background: rgba(255, 255, 255, 0.9);
            color: #333;
            margin: 0 5px;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .stat-box {
            background: rgba(255, 255, 255, 0.1);
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .stat-number {
            font-size: 24px;
            font-weight: bold;
            color: #ffd700;
        }
        .connection-types {
            display: flex;
            gap: 10px;
            margin: 15px 0;
        }
        .connection-btn {
            padding: 8px 16px;
            border: 2px solid rgba(255, 255, 255, 0.3);
            background: transparent;
            color: white;
            border-radius: 20px;
            cursor: pointer;
            transition: all 0.3s ease;
        }
        .connection-btn.active {
            background: rgba(255, 255, 255, 0.2);
            border-color: #ffd700;
        }
        .connection-btn:hover {
            background: rgba(255, 255, 255, 0.1);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🍓 Async Raspberry Pi GPIO Control</h1>
        
        <div class="stats">
            <div class="stat-box">
                <div class="stat-number" id="connectedClients">0</div>
                <div>Connected Clients</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="commandsSent">0</div>
                <div>Commands Sent</div>
            </div>
            <div class="stat-box">
                <div class="stat-number" id="uptime">0s</div>
                <div>Uptime</div>
            </div>
        </div>
        
        <div class="connection-types">
            <button class="connection-btn active" onclick="switchConnection('sse')">Server-Sent Events</button>
            <button class="connection-btn" onclick="switchConnection('ws')">WebSocket</button>
        </div>
        
        <div id="connectionStatus" class="status disconnected">
            Disconnected
        </div>
        
        <div class="pin-group">
            <h3>📤 Output Controls</h3>
            <div>
                <label>Pin: <input type="number" id="outputPin" value="18" min="1" max="40"></label>
                <button class="btn-on" onclick="setOutput(true)">Turn ON</button>
                <button class="btn-off" onclick="setOutput(false)">Turn OFF</button>
                <button class="btn-toggle" onclick="toggleOutput()">Toggle</button>
            </div>
            
            <h4>Quick Controls:</h4>
            <button class="btn-on" onclick="sendCommand('set_output', 18, true)">LED ON (Pin 18)</button>
            <button class="btn-off" onclick="sendCommand('set_output', 18, false)">LED OFF (Pin 18)</button>
            <button class="btn-toggle" onclick="sendCommand('toggle_output', 23)">Toggle Relay (Pin 23)</button>
            <button class="btn-toggle" onclick="sendCommand('toggle_output', 24)">Toggle Motor (Pin 24)</button>
        </div>
        
        <div class="pin-group">
            <h3>📥 Input Reading</h3>
            <div>
                <label>Pin: <input type="number" id="inputPin" value="25" min="1" max="40"></label>
                <button class="btn-read" onclick="getInput()">Read State</button>
            </div>
            
            <h4>Quick Reads:</h4>
            <button class="btn-read" onclick="sendCommand('get_input', 25)">Read Button (Pin 25)</button>
            <button class="btn-read" onclick="sendCommand('get_input', 7)">Read PIR (Pin 7)</button>
            <button class="btn-read" onclick="sendCommand('get_input', 8)">Read Door (Pin 8)</button>
        </div>
        
        <div class="pin-group">
            <h3>📊 System Commands</h3>
            <button class="btn-read" onclick="sendCommand('get_status')">Get All Status</button>
            <button class="btn-read" onclick="sendCommand('ping')">Ping Device</button>
            <button class="btn-bulk" onclick="sendBulkCommands()">Send Bulk Test</button>
        </div>
        
        <div class="pin-group">
            <h3>📋 Activity Log</h3>
            <div id="log" class="log"></div>
            <button onclick="clearLog()">Clear Log</button>
            <button onclick="exportLog()">Export Log</button>
        </div>
    </div>

    <script>
        let eventSource = null;
        let websocket = null;
        let connectionType = 'sse';
        let startTime = Date.now();
        
        function switchConnection(type) {
            // Update button states
            document.querySelectorAll('.connection-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            event.target.classList.add('active');
            
            // Disconnect current connection
            if (eventSource) {
                eventSource.close();
                eventSource = null;
            }
            if (websocket) {
                websocket.close();
                websocket = null;
            }
            
            connectionType = type;
            
            // Connect with new type
            if (type === 'sse') {
                connectSSE();
            } else {
                connectWebSocket();
            }
        }
        
        function connectSSE() {
            eventSource = new EventSource('/events');
            
            eventSource.onopen = function() {
                updateConnectionStatus(true, 'SSE');
                log('🟢 Connected to Raspberry Pi via Server-Sent Events');
            };
            
            eventSource.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    handleServerMessage(data);
                } catch (e) {
                    log('❌ Error parsing server message: ' + e.message);
                }
            };
            
            eventSource.onerror = function() {
                updateConnectionStatus(false, 'SSE');
                log('🔴 SSE connection lost. Attempting to reconnect...');
                
                setTimeout(() => {
                    if (eventSource && eventSource.readyState === EventSource.CLOSED) {
                        connectSSE();
                    }
                }, 5000);
            };
        }
        
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            
            websocket = new WebSocket(wsUrl);
            
            websocket.onopen = function() {
                updateConnectionStatus(true, 'WebSocket');
                log('🟢 Connected to Raspberry Pi via WebSocket');
            };
            
            websocket.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    handleServerMessage(data);
                } catch (e) {
                    log('❌ Error parsing WebSocket message: ' + e.message);
                }
            };
            
            websocket.onclose = function() {
                updateConnectionStatus(false, 'WebSocket');
                log('🔴 WebSocket connection closed. Attempting to reconnect...');
                
                setTimeout(() => {
                    if (connectionType === 'ws') {
                        connectWebSocket();
                    }
                }, 5000);
            };
            
            websocket.onerror = function(error) {
                log('❌ WebSocket error: ' + error);
            };
        }
        
        function updateConnectionStatus(connected, type) {
            const statusDiv = document.getElementById('connectionStatus');
            if (connected) {
                statusDiv.textContent = `Connected to Raspberry Pi via ${type}`;
                statusDiv.className = 'status connected';
            } else {
                statusDiv.textContent = `Disconnected from Raspberry Pi (${type})`;
                statusDiv.className = 'status disconnected';
            }
        }
        
        function handleServerMessage(data) {
            if (data.type === 'connected' || data.type === 'ws_connected') {
                log(`✅ ${data.type === 'ws_connected' ? 'WebSocket' : 'SSE'} connection established, client ID: ${data.client_id}`);
            } else if (data.type === 'heartbeat') {
                // Silent heartbeat - just update stats
                updateStats();
            } else if (data.type === 'interrupt') {
                log(`🔔 Interrupt on GPIO ${data.pin} (${data.description}): ${data.state ? 'HIGH' : 'LOW'}`);
            } else if (data.type === 'pong') {
                log('🏓 Pong received from server');
            } else {
                log('📨 Server message: ' + JSON.stringify(data));
            }
        }
        
        async function sendCommand(type, pin = null, state = null) {
            const command = {
                type: type,
                pin: pin,
                state: state
            };
            
            try {
                const response = await fetch('/api/command', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(command)
                });
                
                const result = await response.json();
                
                if (result.success) {
                    log(`✅ Command sent: ${type} ${pin ? 'pin ' + pin : ''} ${state !== null ? (state ? 'HIGH' : 'LOW') : ''}`);
                } else {
                    log(`❌ Command failed: ${result.error || 'Unknown error'}`);
                }
            } catch (error) {
                log(`❌ Network error: ${error.message}`);
            }
        }
        
        async function sendBulkCommands() {
            const commands = [
                {type: 'ping'},
                {type: 'get_status'},
                {type: 'set_output', pin: 18, state: true},
                {type: 'set_output', pin: 18, state: false}
            ];
            
            try {
                const response = await fetch('/api/commands/bulk', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(commands)
                });
                
                const result = await response.json();
                log(`📦 Bulk commands sent: ${result.total_commands} commands`);
                
            } catch (error) {
                log(`❌ Bulk command error: ${error.message}`);
            }
        }
        
        function setOutput(state) {
            const pin = parseInt(document.getElementById('outputPin').value);
            sendCommand('set_output', pin, state);
        }
        
        function toggleOutput() {
            const pin = parseInt(document.getElementById('outputPin').value);
            sendCommand('toggle_output', pin);
        }
        
        function getInput() {
            const pin = parseInt(document.getElementById('inputPin').value);
            sendCommand('get_input', pin);
        }
        
        async function updateStats() {
            try {
                const response = await fetch('/api/status');
                const status = await response.json();
                
                document.getElementById('connectedClients').textContent = status.connected_clients;
                document.getElementById('commandsSent').textContent = status.commands_sent;
                
                const uptime = Math.floor((Date.now() - startTime) / 1000);
                document.getElementById('uptime').textContent = uptime + 's';
                
            } catch (error) {
                console.warn('Failed to update stats:', error);
            }
        }
        
        function log(message) {
            const logDiv = document.getElementById('log');
            const timestamp = new Date().toLocaleTimeString();
            logDiv.innerHTML += `[${timestamp}] ${message}<br>`;
            logDiv.scrollTop = logDiv.scrollHeight;
        }
        
        function clearLog() {
            document.getElementById('log').innerHTML = '';
        }
        
        function exportLog() {
            const logContent = document.getElementById('log').innerHTML;
            const blob = new Blob([logContent.replace(/<br>/g, '\n').replace(/<[^>]*>/g, '')], {type: 'text/plain'});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'gpio_log_' + new Date().toISOString().slice(0,19).replace(/:/g, '-') + '.txt';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }
        
        // Connect when page loads
        window.addEventListener('load', () => {
            connectSSE();
            updateStats();
            setInterval(updateStats, 5000); // Update stats every 5 seconds
        });
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {
            if (eventSource) {
                eventSource.close();
            }
            if (websocket) {
                websocket.close();
            }
        });
    </script>
</body>
</html>
'''

# Application startup and shutdown events
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting Async GPIO SSE Command Server")
    logger.info("📱 Web interface: http://localhost:8000")
    logger.info("📡 SSE endpoint: http://localhost:8000/events")
    logger.info("🔌 API endpoint: http://localhost:8000/api/command")
    logger.info("🌐 WebSocket endpoint: ws://localhost:8000/ws")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("🛑 Shutting down Async GPIO SSE Command Server")
    # Clean up any remaining client connections
    for client_queue in connected_clients:
        try:
            await client_queue.put({'type': 'server_shutdown', 'message': 'Server is shutting down'})
        except:
            pass

if __name__ == '__main__':
    uvicorn.run(
        "async_sse_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=True
    )
