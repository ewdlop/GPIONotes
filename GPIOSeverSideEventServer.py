#!/usr/bin/env python3
"""
Simple SSE Server for sending GPIO commands to Raspberry Pi
"""

import json
import time
import uuid
from datetime import datetime
from flask import Flask, render_template, request, Response, jsonify
from flask_cors import CORS
import threading
import queue

app = Flask(__name__)
CORS(app)

# Queue for sending commands to connected clients
command_queue = queue.Queue()
connected_clients = []

class SSEClient:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.queue = queue.Queue()
        self.connected = True

clients = {}

@app.route('/')
def index():
    """Main control interface"""
    return render_template('gpio_control.html')

@app.route('/events')
def events():
    """SSE endpoint for GPIO commands"""
    def event_stream():
        client = SSEClient()
        clients[client.id] = client
        
        try:
            # Send initial connection message
            yield f"data: {json.dumps({'type': 'connected', 'client_id': client.id})}\n\n"
            
            while client.connected:
                try:
                    # Check for global commands
                    try:
                        command = command_queue.get_nowait()
                        yield f"data: {json.dumps(command)}\n\n"
                    except queue.Empty:
                        pass
                    
                    # Check for client-specific commands
                    try:
                        command = client.queue.get_nowait()
                        yield f"data: {json.dumps(command)}\n\n"
                    except queue.Empty:
                        pass
                    
                    # Send heartbeat every 30 seconds
                    heartbeat = {
                        'type': 'heartbeat',
                        'timestamp': datetime.now().isoformat()
                    }
                    yield f"data: {json.dumps(heartbeat)}\n\n"
                    
                    time.sleep(30)
                    
                except GeneratorExit:
                    break
                except Exception as e:
                    print(f"Error in event stream: {e}")
                    break
        finally:
            client.connected = False
            if client.id in clients:
                del clients[client.id]
    
    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/command', methods=['POST'])
def send_command():
    """API endpoint for sending GPIO commands"""
    try:
        command = request.json
        command['id'] = str(uuid.uuid4())
        command['timestamp'] = datetime.now().isoformat()
        
        # Broadcast to all connected clients
        command_queue.put(command)
        
        return jsonify({
            'success': True,
            'command_id': command['id'],
            'message': 'Command sent successfully'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/status')
def get_status():
    """Get server status"""
    return jsonify({
        'connected_clients': len(clients),
        'server_time': datetime.now().isoformat(),
        'status': 'running'
    })

# HTML template for the control interface
GPIO_CONTROL_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Raspberry Pi GPIO Control</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f0f0f0; }
        .container { max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 10px; }
        .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
        .status.connected { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .status.disconnected { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        .pin-group { margin: 15px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .pin-group h3 { margin-top: 0; color: #333; }
        button { padding: 10px 15px; margin: 5px; border: none; border-radius: 5px; cursor: pointer; font-size: 14px; }
        .btn-on { background: #28a745; color: white; }
        .btn-off { background: #dc3545; color: white; }
        .btn-toggle { background: #17a2b8; color: white; }
        .btn-read { background: #6c757d; color: white; }
        .log { max-height: 200px; overflow-y: auto; background: #f8f9fa; padding: 10px; border-radius: 5px; font-family: monospace; font-size: 12px; }
        input[type="number"] { width: 60px; padding: 5px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🍓 Raspberry Pi GPIO Control</h1>
        
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
        </div>
        
        <div class="pin-group">
            <h3>📋 Activity Log</h3>
            <div id="log" class="log"></div>
            <button onclick="clearLog()">Clear Log</button>
        </div>
    </div>

    <script>
        let eventSource = null;
        
        function connectSSE() {
            eventSource = new EventSource('/events');
            
            eventSource.onopen = function() {
                updateConnectionStatus(true);
                log('Connected to Raspberry Pi');
            };
            
            eventSource.onmessage = function(event) {
                try {
                    const data = JSON.parse(event.data);
                    handleServerMessage(data);
                } catch (e) {
                    log('Error parsing server message: ' + e.message);
                }
            };
            
            eventSource.onerror = function() {
                updateConnectionStatus(false);
                log('Connection lost. Attempting to reconnect...');
                
                setTimeout(() => {
                    if (eventSource.readyState === EventSource.CLOSED) {
                        connectSSE();
                    }
                }, 5000);
            };
        }
        
        function updateConnectionStatus(connected) {
            const statusDiv = document.getElementById('connectionStatus');
            if (connected) {
                statusDiv.textContent = 'Connected to Raspberry Pi';
                statusDiv.className = 'status connected';
            } else {
                statusDiv.textContent = 'Disconnected from Raspberry Pi';
                statusDiv.className = 'status disconnected';
            }
        }
        
        function handleServerMessage(data) {
            if (data.type === 'connected') {
                log('SSE connection established, client ID: ' + data.client_id);
            } else if (data.type === 'heartbeat') {
                // Silent heartbeat
            } else if (data.type === 'interrupt') {
                log(`🔔 Interrupt on GPIO ${data.pin} (${data.description}): ${data.state ? 'HIGH' : 'LOW'}`);
            } else {
                log('Server message: ' + JSON.stringify(data));
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
                    log(`❌ Command failed: ${result.error}`);
                }
            } catch (error) {
                log(`❌ Network error: ${error.message}`);
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
        
        function log(message) {
            const logDiv = document.getElementById('log');
            const timestamp = new Date().toLocaleTimeString();
            logDiv.innerHTML += `[${timestamp}] ${message}<br>`;
            logDiv.scrollTop = logDiv.scrollHeight;
        }
        
        function clearLog() {
            document.getElementById('log').innerHTML = '';
        }
        
        // Connect when page loads
        window.addEventListener('load', connectSSE);
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {
            if (eventSource) {
                eventSource.close();
            }
        });
    </script>
</body>
</html>
'''

# Create templates directory and file
import os
templates_dir = 'templates'
if not os.path.exists(templates_dir):
    os.makedirs(templates_dir)

with open(os.path.join(templates_dir, 'gpio_control.html'), 'w') as f:
    f.write(GPIO_CONTROL_HTML)

# Example usage and test commands
@app.route('/api/test')
def test_commands():
    """Test endpoint with example commands"""
    test_commands = [
        {'type': 'set_output', 'pin': 18, 'state': True},   # Turn on LED
        {'type': 'get_input', 'pin': 25},                   # Read button
        {'type': 'toggle_output', 'pin': 23},               # Toggle relay
        {'type': 'get_status'},                             # Get all pin status
        {'type': 'ping'}                                    # Ping test
    ]
    
    return jsonify({
        'test_commands': test_commands,
        'usage': 'POST these commands to /api/command to test the GPIO controller'
    })

if __name__ == '__main__':
    print("🌐 Starting SSE GPIO Command Server")
    print("📱 Web interface: http://localhost:8000")
    print("📡 SSE endpoint: http://localhost:8000/events")
    print("🔌 API endpoint: http://localhost:8000/api/command")
    print("🧪 Test commands: http://localhost:8000/api/test")
    
    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
