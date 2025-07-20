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
