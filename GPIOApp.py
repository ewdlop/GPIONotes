#!/usr/bin/env python3
"""
Raspberry Pi GPIO Controller with Server-Sent Events (SSE)
Listens for GPIO commands from a server via SSE and handles device interrupts.
"""

import json
import time
import threading
import logging
import signal
import sys
from typing import Dict, Any, Callable
from dataclasses import dataclass
from datetime import datetime

try:
    import RPi.GPIO as GPIO
except ImportError:
    print("RPi.GPIO not available. Using mock GPIO for testing.")
    # Mock GPIO for testing on non-Pi systems
    class MockGPIO:
        BCM = "BCM"
        IN = "IN"
        OUT = "OUT"
        PUD_UP = "PUD_UP"
        PUD_DOWN = "PUD_DOWN"
        RISING = "RISING"
        FALLING = "FALLING"
        BOTH = "BOTH"
        HIGH = 1
        LOW = 0
        
        @staticmethod
        def setmode(mode): pass
        @staticmethod
        def setup(pin, mode, pull_up_down=None): pass
        @staticmethod
        def output(pin, state): 
            print(f"GPIO {pin} set to {'HIGH' if state else 'LOW'}")
        @staticmethod
        def input(pin): return 0
        @staticmethod
        def add_event_detect(pin, edge, callback=None, bouncetime=None): pass
        @staticmethod
        def cleanup(): pass
    
    GPIO = MockGPIO()

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("Please install requests: pip install requests")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/gpio_controller.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class GPIOPin:
    """Configuration for a GPIO pin"""
    pin: int
    mode: str  # 'in' or 'out'
    pull: str = None  # 'up', 'down', or None
    initial_state: bool = False
    description: str = ""

class GPIOController:
    """Handles GPIO operations and device state management"""
    
    def __init__(self):
        self.pins: Dict[int, GPIOPin] = {}
        self.pin_states: Dict[int, bool] = {}
        self.interrupt_callbacks: Dict[int, Callable] = {}
        
        # Initialize GPIO
        GPIO.setmode(GPIO.BCM)
        logger.info("GPIO initialized in BCM mode")
        
        # Default pin configuration
        self.setup_default_pins()
    
    def setup_default_pins(self):
        """Setup default GPIO pin configuration"""
        default_pins = [
            GPIOPin(18, 'out', description="LED Output"),
            GPIOPin(23, 'out', description="Relay Control"),
            GPIOPin(24, 'out', description="Motor Control"),
            GPIOPin(25, 'in', 'up', description="Button Input"),
            GPIOPin(7, 'in', 'up', description="PIR Sensor"),
            GPIOPin(8, 'in', 'up', description="Door Sensor"),
        ]
        
        for pin_config in default_pins:
            self.setup_pin(pin_config)
    
    def setup_pin(self, pin_config: GPIOPin):
        """Setup a GPIO pin with the given configuration"""
        try:
            self.pins[pin_config.pin] = pin_config
            
            if pin_config.mode == 'out':
                GPIO.setup(pin_config.pin, GPIO.OUT)
                GPIO.output(pin_config.pin, GPIO.HIGH if pin_config.initial_state else GPIO.LOW)
                self.pin_states[pin_config.pin] = pin_config.initial_state
                logger.info(f"Setup GPIO {pin_config.pin} as OUTPUT ({pin_config.description})")
                
            elif pin_config.mode == 'in':
                pull_mode = None
                if pin_config.pull == 'up':
                    pull_mode = GPIO.PUD_UP
                elif pin_config.pull == 'down':
                    pull_mode = GPIO.PUD_DOWN
                
                GPIO.setup(pin_config.pin, GPIO.IN, pull_up_down=pull_mode)
                
                # Setup interrupt detection for input pins
                self.setup_interrupt(pin_config.pin)
                logger.info(f"Setup GPIO {pin_config.pin} as INPUT ({pin_config.description})")
                
        except Exception as e:
            logger.error(f"Failed to setup GPIO {pin_config.pin}: {e}")
    
    def setup_interrupt(self, pin: int, edge: str = 'both', bouncetime: int = 200):
        """Setup interrupt detection for a GPIO pin"""
        try:
            edge_type = GPIO.BOTH
            if edge.lower() == 'rising':
                edge_type = GPIO.RISING
            elif edge.lower() == 'falling':
                edge_type = GPIO.FALLING
            
            GPIO.add_event_detect(
                pin, 
                edge_type, 
                callback=lambda channel: self.handle_interrupt(channel),
                bouncetime=bouncetime
            )
            logger.info(f"Setup interrupt detection on GPIO {pin} (edge: {edge})")
            
        except Exception as e:
            logger.error(f"Failed to setup interrupt on GPIO {pin}: {e}")
    
    def handle_interrupt(self, pin: int):
        """Handle GPIO interrupt"""
        try:
            state = GPIO.input(pin)
            timestamp = datetime.now().isoformat()
            
            pin_info = self.pins.get(pin, GPIOPin(pin, 'in', description="Unknown"))
            
            logger.info(f"Interrupt on GPIO {pin} ({pin_info.description}): {'HIGH' if state else 'LOW'}")
            
            # Send interrupt notification to server
            interrupt_data = {
                'type': 'interrupt',
                'pin': pin,
                'state': bool(state),
                'timestamp': timestamp,
                'description': pin_info.description
            }
            
            # Queue the interrupt for sending to server
            self.send_interrupt_notification(interrupt_data)
            
            # Execute custom callback if registered
            if pin in self.interrupt_callbacks:
                try:
                    self.interrupt_callbacks[pin](pin, state)
                except Exception as e:
                    logger.error(f"Error in interrupt callback for pin {pin}: {e}")
                    
        except Exception as e:
            logger.error(f"Error handling interrupt on pin {pin}: {e}")
    
    def set_output(self, pin: int, state: bool) -> bool:
        """Set the state of an output GPIO pin"""
        try:
            if pin not in self.pins:
                logger.error(f"GPIO {pin} not configured")
                return False
            
            if self.pins[pin].mode != 'out':
                logger.error(f"GPIO {pin} is not configured as output")
                return False
            
            GPIO.output(pin, GPIO.HIGH if state else GPIO.LOW)
            self.pin_states[pin] = state
            
            pin_info = self.pins[pin]
            logger.info(f"GPIO {pin} ({pin_info.description}) set to {'HIGH' if state else 'LOW'}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set GPIO {pin} to {state}: {e}")
            return False
    
    def get_input(self, pin: int) -> bool:
        """Get the state of an input GPIO pin"""
        try:
            if pin not in self.pins:
                logger.error(f"GPIO {pin} not configured")
                return False
            
            if self.pins[pin].mode != 'in':
                logger.error(f"GPIO {pin} is not configured as input")
                return False
            
            state = bool(GPIO.input(pin))
            return state
            
        except Exception as e:
            logger.error(f"Failed to read GPIO {pin}: {e}")
            return False
    
    def toggle_output(self, pin: int) -> bool:
        """Toggle the state of an output GPIO pin"""
        if pin in self.pin_states:
            new_state = not self.pin_states[pin]
            return self.set_output(pin, new_state)
        return False
    
    def get_pin_status(self) -> Dict[str, Any]:
        """Get the status of all configured pins"""
        status = {
            'outputs': {},
            'inputs': {},
            'timestamp': datetime.now().isoformat()
        }
        
        for pin, config in self.pins.items():
            pin_data = {
                'pin': pin,
                'description': config.description,
                'state': None
            }
            
            try:
                if config.mode == 'out':
                    pin_data['state'] = self.pin_states.get(pin, False)
                    status['outputs'][str(pin)] = pin_data
                else:
                    pin_data['state'] = self.get_input(pin)
                    status['inputs'][str(pin)] = pin_data
            except Exception as e:
                pin_data['error'] = str(e)
                if config.mode == 'out':
                    status['outputs'][str(pin)] = pin_data
                else:
                    status['inputs'][str(pin)] = pin_data
        
        return status
    
    def register_interrupt_callback(self, pin: int, callback: Callable):
        """Register a custom callback for pin interrupts"""
        self.interrupt_callbacks[pin] = callback
        logger.info(f"Registered interrupt callback for GPIO {pin}")
    
    def send_interrupt_notification(self, interrupt_data: Dict[str, Any]):
        """Send interrupt notification to server (implement based on your server)"""
        # This could send to a webhook, MQTT broker, or queue for the SSE client
        logger.info(f"Interrupt notification: {interrupt_data}")
        # TODO: Implement actual notification mechanism

    def cleanup(self):
        """Cleanup GPIO resources"""
        logger.info("Cleaning up GPIO...")
        GPIO.cleanup()

class SSEClient:
    """Server-Sent Events client for receiving GPIO commands"""
    
    def __init__(self, server_url: str, gpio_controller: GPIOController):
        self.server_url = server_url
        self.gpio_controller = gpio_controller
        self.session = requests.Session()
        self.running = False
        
        # Setup retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def connect(self):
        """Connect to SSE server and listen for events"""
        self.running = True
        
        while self.running:
            try:
                logger.info(f"Connecting to SSE server: {self.server_url}")
                
                response = self.session.get(
                    self.server_url,
                    headers={'Accept': 'text/event-stream'},
                    stream=True,
                    timeout=(10, 60)  # Connect timeout, read timeout
                )
                
                if response.status_code == 200:
                    logger.info("Connected to SSE server successfully")
                    self.process_events(response)
                else:
                    logger.error(f"SSE connection failed with status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"SSE connection error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error in SSE client: {e}")
            
            if self.running:
                logger.info("Reconnecting in 5 seconds...")
                time.sleep(5)
    
    def process_events(self, response):
        """Process incoming SSE events"""
        try:
            for line in response.iter_lines(decode_unicode=True):
                if not self.running:
                    break
                
                if line.startswith('data: '):
                    data = line[6:]  # Remove 'data: ' prefix
                    try:
                        event_data = json.loads(data)
                        self.handle_command(event_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON data: {data}, error: {e}")
                    except Exception as e:
                        logger.error(f"Error processing event data: {e}")
                        
        except Exception as e:
            logger.error(f"Error processing SSE events: {e}")
    
    def handle_command(self, command: Dict[str, Any]):
        """Handle incoming GPIO commands"""
        try:
            cmd_type = command.get('type')
            pin = command.get('pin')
            
            logger.info(f"Received command: {command}")
            
            if cmd_type == 'set_output':
                state = command.get('state', False)
                success = self.gpio_controller.set_output(pin, state)
                self.send_response(command.get('id'), success, f"GPIO {pin} set to {state}")
                
            elif cmd_type == 'get_input':
                state = self.gpio_controller.get_input(pin)
                self.send_response(command.get('id'), True, f"GPIO {pin} state: {state}", {'state': state})
                
            elif cmd_type == 'toggle_output':
                success = self.gpio_controller.toggle_output(pin)
                new_state = self.gpio_controller.pin_states.get(pin, False)
                self.send_response(command.get('id'), success, f"GPIO {pin} toggled to {new_state}", {'state': new_state})
                
            elif cmd_type == 'get_status':
                status = self.gpio_controller.get_pin_status()
                self.send_response(command.get('id'), True, "Status retrieved", status)
                
            elif cmd_type == 'ping':
                self.send_response(command.get('id'), True, "pong")
                
            else:
                logger.warning(f"Unknown command type: {cmd_type}")
                self.send_response(command.get('id'), False, f"Unknown command: {cmd_type}")
                
        except Exception as e:
            logger.error(f"Error handling command {command}: {e}")
            self.send_response(command.get('id'), False, f"Command failed: {str(e)}")
    
    def send_response(self, command_id: str, success: bool, message: str, data: Dict[str, Any] = None):
        """Send response back to server (implement based on your server setup)"""
        response = {
            'command_id': command_id,
            'success': success,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'data': data or {}
        }
        
        logger.info(f"Response: {response}")
        # TODO: Implement actual response mechanism (HTTP POST, MQTT, etc.)
    
    def stop(self):
        """Stop the SSE client"""
        self.running = False
        logger.info("SSE client stopped")

class GPIOApp:
    """Main application class"""
    
    def __init__(self, server_url: str = "http://localhost:8000/events"):
        self.gpio_controller = GPIOController()
        self.sse_client = SSEClient(server_url, self.gpio_controller)
        self.running = False
        
        # Setup custom interrupt callbacks
        self.setup_custom_callbacks()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def setup_custom_callbacks(self):
        """Setup custom interrupt callbacks for specific pins"""
        
        # Button press callback (GPIO 25)
        def button_callback(pin, state):
            if not state:  # Button pressed (LOW with pull-up)
                logger.info("Button pressed! Toggling LED...")
                self.gpio_controller.toggle_output(18)  # Toggle LED on pin 18
        
        # PIR sensor callback (GPIO 7)
        def pir_callback(pin, state):
            if state:  # Motion detected
                logger.info("Motion detected! Turning on lights...")
                self.gpio_controller.set_output(23, True)  # Turn on relay
                # Auto turn off after 30 seconds
                threading.Timer(30.0, lambda: self.gpio_controller.set_output(23, False)).start()
        
        # Door sensor callback (GPIO 8)
        def door_callback(pin, state):
            if not state:  # Door opened (LOW with pull-up)
                logger.info("Door opened! Security alert...")
                # Flash LED 5 times
                for i in range(5):
                    self.gpio_controller.set_output(18, True)
                    time.sleep(0.2)
                    self.gpio_controller.set_output(18, False)
                    time.sleep(0.2)
        
        # Register callbacks
        self.gpio_controller.register_interrupt_callback(25, button_callback)
        self.gpio_controller.register_interrupt_callback(7, pir_callback)
        self.gpio_controller.register_interrupt_callback(8, door_callback)
    
    def run(self):
        """Run the main application"""
        self.running = True
        logger.info("Starting Raspberry Pi GPIO Controller")
        
        # Start SSE client in a separate thread
        sse_thread = threading.Thread(target=self.sse_client.connect, daemon=True)
        sse_thread.start()
        
        # Main loop
        try:
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        finally:
            self.cleanup()
    
    def signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        self.sse_client.stop()
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")
        self.sse_client.stop()
        self.gpio_controller.cleanup()
        logger.info("Cleanup complete")

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Raspberry Pi GPIO Controller with SSE')
    parser.add_argument('--server', '-s', default='http://localhost:8000/events',
                       help='SSE server URL (default: http://localhost:8000/events)')
    parser.add_argument('--log-level', '-l', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create and run the application
    app = GPIOApp(args.server)
    
    try:
        app.run()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
