#!/usr/bin/env python3
"""
Raspberry Pi GPIO Controller with Server-Sent Events (SSE) using AsyncIO
Listens for GPIO commands from a server via SSE and handles device interrupts.
"""

import json
import asyncio
import logging
import signal
import sys
from typing import Dict, Any, Callable, Optional
from dataclasses import dataclass
from datetime import datetime
import concurrent.futures
import threading
import queue

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
    import aiohttp
    import aiofiles
except ImportError:
    print("Please install aiohttp and aiofiles: pip install aiohttp aiofiles")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
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

class AsyncGPIOController:
    """Handles GPIO operations and device state management with AsyncIO"""
    
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.pins: Dict[int, GPIOPin] = {}
        self.pin_states: Dict[int, bool] = {}
        self.interrupt_callbacks: Dict[int, Callable] = {}
        self.interrupt_queue = asyncio.Queue()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        
        # Initialize GPIO in thread pool since it's not async
        self.loop.run_in_executor(self.executor, self._init_gpio)
    
    def _init_gpio(self):
        """Initialize GPIO in thread pool"""
        GPIO.setmode(GPIO.BCM)
        logger.info("GPIO initialized in BCM mode")
        
        # Setup default pins
        self._setup_default_pins()
    
    def _setup_default_pins(self):
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
            self._setup_pin(pin_config)
    
    def _setup_pin(self, pin_config: GPIOPin):
        """Setup a GPIO pin with the given configuration (runs in thread)"""
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
                self._setup_interrupt(pin_config.pin)
                logger.info(f"Setup GPIO {pin_config.pin} as INPUT ({pin_config.description})")
                
        except Exception as e:
            logger.error(f"Failed to setup GPIO {pin_config.pin}: {e}")
    
    def _setup_interrupt(self, pin: int, edge: str = 'both', bouncetime: int = 200):
        """Setup interrupt detection for a GPIO pin (runs in thread)"""
        try:
            edge_type = GPIO.BOTH
            if edge.lower() == 'rising':
                edge_type = GPIO.RISING
            elif edge.lower() == 'falling':
                edge_type = GPIO.FALLING
            
            GPIO.add_event_detect(
                pin, 
                edge_type, 
                callback=lambda channel: self._handle_interrupt_sync(channel),
                bouncetime=bouncetime
            )
            logger.info(f"Setup interrupt detection on GPIO {pin} (edge: {edge})")
            
        except Exception as e:
            logger.error(f"Failed to setup interrupt on GPIO {pin}: {e}")
    
    def _handle_interrupt_sync(self, pin: int):
        """Synchronous interrupt handler that queues interrupt for async processing"""
        try:
            state = GPIO.input(pin)
            timestamp = datetime.now().isoformat()
            
            interrupt_data = {
                'pin': pin,
                'state': bool(state),
                'timestamp': timestamp
            }
            
            # Queue interrupt for async processing
            asyncio.run_coroutine_threadsafe(
                self.interrupt_queue.put(interrupt_data), 
                self.loop
            )
            
        except Exception as e:
            logger.error(f"Error in sync interrupt handler for pin {pin}: {e}")
    
    async def handle_interrupt(self, interrupt_data: Dict[str, Any]):
        """Handle GPIO interrupt asynchronously"""
        try:
            pin = interrupt_data['pin']
            state = interrupt_data['state']
            timestamp = interrupt_data['timestamp']
            
            pin_info = self.pins.get(pin, GPIOPin(pin, 'in', description="Unknown"))
            
            logger.info(f"Interrupt on GPIO {pin} ({pin_info.description}): {'HIGH' if state else 'LOW'}")
            
            # Send interrupt notification to server
            notification_data = {
                'type': 'interrupt',
                'pin': pin,
                'state': state,
                'timestamp': timestamp,
                'description': pin_info.description
            }
            
            # Send interrupt notification
            await self.send_interrupt_notification(notification_data)
            
            # Execute custom callback if registered
            if pin in self.interrupt_callbacks:
                try:
                    callback = self.interrupt_callbacks[pin]
                    if asyncio.iscoroutinefunction(callback):
                        await callback(pin, state)
                    else:
                        # Run sync callback in executor
                        await self.loop.run_in_executor(self.executor, callback, pin, state)
                except Exception as e:
                    logger.error(f"Error in interrupt callback for pin {pin}: {e}")
                    
        except Exception as e:
            logger.error(f"Error handling interrupt: {e}")
    
    async def set_output(self, pin: int, state: bool) -> bool:
        """Set the state of an output GPIO pin"""
        try:
            if pin not in self.pins:
                logger.error(f"GPIO {pin} not configured")
                return False
            
            if self.pins[pin].mode != 'out':
                logger.error(f"GPIO {pin} is not configured as output")
                return False
            
            # Run GPIO operation in executor
            await self.loop.run_in_executor(
                self.executor, 
                GPIO.output, 
                pin, 
                GPIO.HIGH if state else GPIO.LOW
            )
            
            self.pin_states[pin] = state
            
            pin_info = self.pins[pin]
            logger.info(f"GPIO {pin} ({pin_info.description}) set to {'HIGH' if state else 'LOW'}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set GPIO {pin} to {state}: {e}")
            return False
    
    async def get_input(self, pin: int) -> bool:
        """Get the state of an input GPIO pin"""
        try:
            if pin not in self.pins:
                logger.error(f"GPIO {pin} not configured")
                return False
            
            if self.pins[pin].mode != 'in':
                logger.error(f"GPIO {pin} is not configured as input")
                return False
            
            # Run GPIO operation in executor
            state = await self.loop.run_in_executor(self.executor, GPIO.input, pin)
            return bool(state)
            
        except Exception as e:
            logger.error(f"Failed to read GPIO {pin}: {e}")
            return False
    
    async def toggle_output(self, pin: int) -> bool:
        """Toggle the state of an output GPIO pin"""
        if pin in self.pin_states:
            new_state = not self.pin_states[pin]
            return await self.set_output(pin, new_state)
        return False
    
    async def get_pin_status(self) -> Dict[str, Any]:
        """Get the status of all configured pins"""
        status = {
            'outputs': {},
            'inputs': {},
            'timestamp': datetime.now().isoformat()
        }
        
        # Process all pins concurrently
        tasks = []
        for pin, config in self.pins.items():
            tasks.append(self._get_pin_data(pin, config))
        
        pin_data_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for pin_data in pin_data_list:
            if isinstance(pin_data, Exception):
                logger.error(f"Error getting pin data: {pin_data}")
                continue
                
            pin = pin_data['pin']
            config = self.pins[pin]
            
            if config.mode == 'out':
                status['outputs'][str(pin)] = pin_data
            else:
                status['inputs'][str(pin)] = pin_data
        
        return status
    
    async def _get_pin_data(self, pin: int, config: GPIOPin) -> Dict[str, Any]:
        """Get data for a single pin"""
        pin_data = {
            'pin': pin,
            'description': config.description,
            'state': None
        }
        
        try:
            if config.mode == 'out':
                pin_data['state'] = self.pin_states.get(pin, False)
            else:
                pin_data['state'] = await self.get_input(pin)
        except Exception as e:
            pin_data['error'] = str(e)
        
        return pin_data
    
    def register_interrupt_callback(self, pin: int, callback: Callable):
        """Register a custom callback for pin interrupts"""
        self.interrupt_callbacks[pin] = callback
        logger.info(f"Registered interrupt callback for GPIO {pin}")
    
    async def send_interrupt_notification(self, interrupt_data: Dict[str, Any]):
        """Send interrupt notification (implement based on your server)"""
        logger.info(f"Interrupt notification: {interrupt_data}")
        # TODO: Implement actual notification mechanism (webhook, MQTT, etc.)
    
    async def process_interrupts(self):
        """Process interrupts from the queue"""
        while True:
            try:
                interrupt_data = await self.interrupt_queue.get()
                await self.handle_interrupt(interrupt_data)
                self.interrupt_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing interrupt: {e}")
    
    async def cleanup(self):
        """Cleanup GPIO resources"""
        logger.info("Cleaning up GPIO...")
        await self.loop.run_in_executor(self.executor, GPIO.cleanup)
        self.executor.shutdown(wait=True)

class AsyncSSEClient:
    """Async Server-Sent Events client for receiving GPIO commands"""
    
    def __init__(self, server_url: str, gpio_controller: AsyncGPIOController):
        self.server_url = server_url
        self.gpio_controller = gpio_controller
        self.session: Optional[aiohttp.ClientSession] = None
        self.running = False
        self.reconnect_delay = 5
        self.max_reconnect_delay = 300
    
    async def connect(self):
        """Connect to SSE server and listen for events"""
        self.running = True
        current_delay = self.reconnect_delay
        
        # Setup aiohttp session with retry configuration
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={'Accept': 'text/event-stream'}
        )
        
        while self.running:
            try:
                logger.info(f"Connecting to SSE server: {self.server_url}")
                
                async with self.session.get(self.server_url) as response:
                    if response.status == 200:
                        logger.info("Connected to SSE server successfully")
                        current_delay = self.reconnect_delay  # Reset delay on successful connection
                        await self.process_events(response)
                    else:
                        logger.error(f"SSE connection failed with status {response.status}")
                        
            except aiohttp.ClientError as e:
                logger.error(f"SSE connection error: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in SSE client: {e}")
            
            if self.running:
                logger.info(f"Reconnecting in {current_delay} seconds...")
                await asyncio.sleep(current_delay)
                # Exponential backoff with jitter
                current_delay = min(current_delay * 1.5, self.max_reconnect_delay)
    
    async def process_events(self, response: aiohttp.ClientResponse):
        """Process incoming SSE events"""
        try:
            async for line in response.content:
                if not self.running:
                    break
                
                line_str = line.decode('utf-8').strip()
                
                if line_str.startswith('data: '):
                    data = line_str[6:]  # Remove 'data: ' prefix
                    try:
                        event_data = json.loads(data)
                        await self.handle_command(event_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON data: {data}, error: {e}")
                    except Exception as e:
                        logger.error(f"Error processing event data: {e}")
                        
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error processing SSE events: {e}")
    
    async def handle_command(self, command: Dict[str, Any]):
        """Handle incoming GPIO commands"""
        try:
            cmd_type = command.get('type')
            pin = command.get('pin')
            
            logger.info(f"Received command: {command}")
            
            if cmd_type == 'set_output':
                state = command.get('state', False)
                success = await self.gpio_controller.set_output(pin, state)
                await self.send_response(command.get('id'), success, f"GPIO {pin} set to {state}")
                
            elif cmd_type == 'get_input':
                state = await self.gpio_controller.get_input(pin)
                await self.send_response(command.get('id'), True, f"GPIO {pin} state: {state}", {'state': state})
                
            elif cmd_type == 'toggle_output':
                success = await self.gpio_controller.toggle_output(pin)
                new_state = self.gpio_controller.pin_states.get(pin, False)
                await self.send_response(command.get('id'), success, f"GPIO {pin} toggled to {new_state}", {'state': new_state})
                
            elif cmd_type == 'get_status':
                status = await self.gpio_controller.get_pin_status()
                await self.send_response(command.get('id'), True, "Status retrieved", status)
                
            elif cmd_type == 'ping':
                await self.send_response(command.get('id'), True, "pong")
                
            elif cmd_type == 'heartbeat':
                # Silent heartbeat processing
                pass
                
            else:
                logger.warning(f"Unknown command type: {cmd_type}")
                await self.send_response(command.get('id'), False, f"Unknown command: {cmd_type}")
                
        except Exception as e:
            logger.error(f"Error handling command {command}: {e}")
            await self.send_response(command.get('id'), False, f"Command failed: {str(e)}")
    
    async def send_response(self, command_id: str, success: bool, message: str, data: Dict[str, Any] = None):
        """Send response back to server"""
        response = {
            'command_id': command_id,
            'success': success,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'data': data or {}
        }
        
        logger.info(f"Response: {response}")
        # TODO: Implement actual response mechanism (HTTP POST, WebSocket, MQTT, etc.)
    
    async def stop(self):
        """Stop the SSE client"""
        self.running = False
        if self.session:
            await self.session.close()
        logger.info("SSE client stopped")

class AsyncGPIOApp:
    """Main application class using AsyncIO"""
    
    def __init__(self, server_url: str = "http://localhost:8000/events"):
        self.server_url = server_url
        self.loop = asyncio.get_event_loop()
        self.gpio_controller = AsyncGPIOController(self.loop)
        self.sse_client = AsyncSSEClient(server_url, self.gpio_controller)
        self.running = False
        self.tasks = []
        
        # Setup custom interrupt callbacks
        self.setup_custom_callbacks()
        
        # Setup signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
    
    def setup_custom_callbacks(self):
        """Setup custom interrupt callbacks for specific pins"""
        
        # Button press callback (GPIO 25)
        async def button_callback(pin, state):
            if not state:  # Button pressed (LOW with pull-up)
                logger.info("Button pressed! Toggling LED...")
                await self.gpio_controller.toggle_output(18)  # Toggle LED on pin 18
        
        # PIR sensor callback (GPIO 7)
        async def pir_callback(pin, state):
            if state:  # Motion detected
                logger.info("Motion detected! Turning on lights...")
                await self.gpio_controller.set_output(23, True)  # Turn on relay
                
                # Auto turn off after 30 seconds
                async def auto_turn_off():
                    await asyncio.sleep(30)
                    await self.gpio_controller.set_output(23, False)
                
                asyncio.create_task(auto_turn_off())
        
        # Door sensor callback (GPIO 8)
        async def door_callback(pin, state):
            if not state:  # Door opened (LOW with pull-up)
                logger.info("Door opened! Security alert...")
                # Flash LED 5 times
                for i in range(5):
                    await self.gpio_controller.set_output(18, True)
                    await asyncio.sleep(0.2)
                    await self.gpio_controller.set_output(18, False)
                    await asyncio.sleep(0.2)
        
        # Register callbacks
        self.gpio_controller.register_interrupt_callback(25, button_callback)
        self.gpio_controller.register_interrupt_callback(7, pir_callback)
        self.gpio_controller.register_interrupt_callback(8, door_callback)
    
    async def run(self):
        """Run the main application"""
        self.running = True
        logger.info("Starting Async Raspberry Pi GPIO Controller")
        
        # Start background tasks
        self.tasks = [
            asyncio.create_task(self.sse_client.connect(), name="sse_client"),
            asyncio.create_task(self.gpio_controller.process_interrupts(), name="interrupt_processor"),
            asyncio.create_task(self.health_check(), name="health_check"),
        ]
        
        try:
            # Wait for all tasks to complete
            await asyncio.gather(*self.tasks, return_exceptions=True)
                
        except asyncio.CancelledError:
            logger.info("Application cancelled")
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        finally:
            await self.cleanup()
    
    async def health_check(self):
        """Periodic health check and maintenance"""
        while self.running:
            try:
                # Log system status every 5 minutes
                await asyncio.sleep(300)
                if self.running:
                    status = await self.gpio_controller.get_pin_status()
                    logger.debug(f"Health check - Pin status: {len(status['outputs'])} outputs, {len(status['inputs'])} inputs")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check: {e}")
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down...")
        self.running = False
        
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Stop SSE client
        await self.sse_client.stop()
        
        # Wait for tasks to complete
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Stop the event loop
        self.loop.stop()
    
    async def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")
        await self.sse_client.stop()
        await self.gpio_controller.cleanup()
        logger.info("Cleanup complete")

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Async Raspberry Pi GPIO Controller with SSE')
    parser.add_argument('--server', '-s', default='http://localhost:8000/events',
                       help='SSE server URL (default: http://localhost:8000/events)')
    parser.add_argument('--log-level', '-l', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create and run the application
    app = AsyncGPIOApp(args.server)
    
    try:
        await app.run()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
