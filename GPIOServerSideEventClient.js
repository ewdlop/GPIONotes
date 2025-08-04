#!/usr/bin/env node

/**
 * Raspberry Pi GPIO SSE Client with Direct GPIO Control
 * Listens for commands from SSE server and executes them directly on GPIO pins
 */

const EventSource = require('eventsource');
const gpio = require('rpi-gpio');
const readline = require('readline');
const chalk = require('chalk');
const { performance } = require('perf_hooks');

class RPiGPIOSSEClient {
    constructor(serverUrl = 'http://localhost:8000') {
        this.serverUrl = serverUrl;
        this.sseUrl = `${serverUrl}/events`;
        
        this.eventSource = null;
        this.connected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 5000;
        this.startTime = performance.now();
        
        // GPIO state tracking
        this.setupPins = new Map(); // Track configured pins and their directions
        this.pinStates = new Map(); // Track current pin states
        this.watchedPins = new Map(); // Track pins being watched for interrupts
        
        // Statistics
        this.stats = {
            messagesReceived: 0,
            commandsExecuted: 0,
            gpioOperations: 0,
            interrupts: 0,
            reconnections: 0,
            errors: 0
        };
        
        // Setup readline interface for local commands
        this.rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout,
            prompt: chalk.cyan('rpi-gpio> ')
        });
        
        this.setupEventHandlers();
        this.setupLocalCommands();
        this.initializeGPIO();
    }
    
    async initializeGPIO() {
        try {
            // Set GPIO numbering mode to BCM
            gpio.setMode(gpio.MODE_BCM);
            this.log('success', '🔧 GPIO initialized in BCM mode');
            
            // Setup common pins with default configurations
            await this.setupCommonPins();
            
        } catch (error) {
            this.log('error', `GPIO initialization failed: ${error.message}`);
            this.stats.errors++;
        }
    }
    
    async setupCommonPins() {
        // Common pin configurations for typical IoT devices
        const commonPins = [
            { pin: 18, direction: 'out', description: 'LED' },
            { pin: 23, direction: 'out', description: 'Relay' },
            { pin: 24, direction: 'out', description: 'Buzzer' },
            { pin: 25, direction: 'in', description: 'Button', pullResistor: 'pullup' },
            { pin: 7, direction: 'in', description: 'PIR Sensor' },
            { pin: 8, direction: 'in', description: 'Door Sensor' },
            { pin: 12, direction: 'in', description: 'Motion Sensor' }
        ];
        
        for (const config of commonPins) {
            try {
                await this.setupPin(config.pin, config.direction, config.pullResistor);
                this.log('info', `📌 Pin ${config.pin} configured as ${config.direction} (${config.description})`);
                
                // Watch input pins for changes
                if (config.direction === 'in') {
                    await this.watchPin(config.pin, config.description);
                }
            } catch (error) {
                this.log('warn', `Failed to setup pin ${config.pin}: ${error.message}`);
            }
        }
    }
    
    setupEventHandlers() {
        // Handle process termination
        process.on('SIGINT', () => this.shutdown());
        process.on('SIGTERM', () => this.shutdown());
        process.on('uncaughtException', (error) => {
            this.log('error', `Uncaught exception: ${error.message}`);
            this.shutdown();
        });
        
        // Setup readline handlers
        this.rl.on('line', (input) => {
            this.handleLocalCommand(input.trim());
            this.rl.prompt();
        });
        
        this.rl.on('close', () => {
            this.shutdown();
        });
    }
    
    setupLocalCommands() {
        this.localCommands = {
            help: () => this.showHelp(),
            connect: () => this.connect(),
            disconnect: () => this.disconnect(),
            status: () => this.showStatus(),
            stats: () => this.showStats(),
            clear: () => console.clear(),
            
            // Direct GPIO commands (bypass server)
            on: (pin) => this.setOutput(parseInt(pin), true),
            off: (pin) => this.setOutput(parseInt(pin), false),
            toggle: (pin) => this.toggleOutput(parseInt(pin)),
            read: (pin) => this.readInput(parseInt(pin)),
            setup: (pin, direction) => this.setupPin(parseInt(pin), direction),
            watch: (pin) => this.watchPin(parseInt(pin)),
            unwatch: (pin) => this.unwatchPin(parseInt(pin)),
            
            // Quick presets
            led: {
                on: () => this.setOutput(18, true),
                off: () => this.setOutput(18, false),
                blink: (times) => this.blinkLED(parseInt(times) || 5)
            },
            relay: {
                on: () => this.setOutput(23, true),
                off: () => this.setOutput(23, false),
                toggle: () => this.toggleOutput(23)
            },
            buzzer: {
                on: () => this.setOutput(24, true),
                off: () => this.setOutput(24, false),
                beep: (times) => this.beepBuzzer(parseInt(times) || 3)
            },
            button: () => this.readInput(25),
            pir: () => this.readInput(7),
            door: () => this.readInput(8),
            
            // Test commands
            test: () => this.runTests(),
            monitor: (duration) => this.startMonitoring(parseInt(duration) || 30)
        };
    }
    
    async connect() {
        if (this.connected) {
            this.log('warn', 'Already connected to SSE server');
            return;
        }
        
        try {
            this.log('info', `Connecting to SSE server: ${this.sseUrl}`);
            
            this.eventSource = new EventSource(this.sseUrl, {
                headers: {
                    'Accept': 'text/event-stream',
                    'Cache-Control': 'no-cache'
                }
            });
            
            this.eventSource.onopen = () => {
                this.connected = true;
                this.reconnectAttempts = 0;
                this.log('success', '✅ Connected to SSE server');
                this.rl.prompt();
            };
            
            this.eventSource.onmessage = (event) => {
                this.handleSSEMessage(event);
            };
            
            this.eventSource.onerror = (error) => {
                this.handleSSEError(error);
            };
            
        } catch (error) {
            this.log('error', `Connection failed: ${error.message}`);
            this.stats.errors++;
        }
    }
    
    disconnect() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
        this.connected = false;
        this.log('info', '🔴 Disconnected from SSE server');
    }
    
    async handleSSEMessage(event) {
        try {
            const data = JSON.parse(event.data);
            this.stats.messagesReceived++;
            
            switch (data.type) {
                case 'connected':
                    this.log('success', `Connected with client ID: ${data.client_id}`);
                    break;
                    
                case 'heartbeat':
                    // Silent heartbeat
                    break;
                    
                case 'gpio_command':
                    await this.executeGPIOCommand(data);
                    break;
                    
                case 'setup_pin':
                    await this.setupPin(data.pin, data.direction, data.pullResistor);
                    break;
                    
                case 'watch_pin':
                    await this.watchPin(data.pin, data.description);
                    break;
                    
                case 'bulk_commands':
                    await this.executeBulkCommands(data.commands);
                    break;
                    
                default:
                    this.log('info', `📨 Unknown message type: ${data.type}`);
            }
            
        } catch (error) {
            this.log('error', `Failed to parse SSE message: ${error.message}`);
            this.stats.errors++;
        }
    }
    
    async executeGPIOCommand(data) {
        try {
            const { command, pin, state, value } = data;
            this.stats.commandsExecuted++;
            
            switch (command) {
                case 'set_output':
                    await this.setOutput(pin, state);
                    this.log('success', `🔧 Set pin ${pin} to ${state ? 'HIGH' : 'LOW'}`);
                    break;
                    
                case 'get_input':
                    const inputState = await this.readInput(pin);
                    this.log('info', `📖 Pin ${pin} state: ${inputState ? 'HIGH' : 'LOW'}`);
                    break;
                    
                case 'toggle_output':
                    await this.toggleOutput(pin);
                    break;
                    
                case 'pwm':
                    // Note: Basic GPIO doesn't support PWM, would need pigpio for hardware PWM
                    this.log('warn', `PWM not supported with rpi-gpio. Pin ${pin} ignored.`);
                    break;
                    
                default:
                    this.log('error', `Unknown GPIO command: ${command}`);
            }
            
        } catch (error) {
            this.log('error', `GPIO command failed: ${error.message}`);
            this.stats.errors++;
        }
    }
    
    async executeBulkCommands(commands) {
        this.log('info', `📦 Executing ${commands.length} bulk commands...`);
        
        for (const cmd of commands) {
            try {
                await this.executeGPIOCommand(cmd);
                await this.sleep(50); // Small delay between commands
            } catch (error) {
                this.log('error', `Bulk command failed: ${error.message}`);
            }
        }
        
        this.log('success', `📦 Bulk commands completed`);
    }
    
    handleSSEError(error) {
        this.connected = false;
        this.stats.errors++;
        
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            this.stats.reconnections++;
            
            this.log('warn', `Connection lost. Reconnecting in ${this.reconnectDelay/1000}s... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
            
            setTimeout(() => {
                this.connect();
            }, this.reconnectDelay);
            
            // Exponential backoff
            this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, 60000);
        } else {
            this.log('error', '❌ Max reconnection attempts reached. Use "connect" to retry.');
        }
    }
    
    async setupPin(pin, direction, pullResistor = null) {
        return new Promise((resolve, reject) => {
            const options = {};
            if (pullResistor) {
                options.pullResistor = pullResistor === 'pullup' ? gpio.PULL_UP : gpio.PULL_DOWN;
            }
            
            gpio.setup(pin, direction === 'out' ? gpio.DIR_OUT : gpio.DIR_IN, options, (err) => {
                if (err) {
                    reject(err);
                } else {
                    this.setupPins.set(pin, { direction, pullResistor });
                    this.stats.gpioOperations++;
                    resolve();
                }
            });
        });
    }
    
    async setOutput(pin, state) {
        if (!this.setupPins.has(pin) || this.setupPins.get(pin).direction !== 'out') {
            await this.setupPin(pin, 'out');
        }
        
        return new Promise((resolve, reject) => {
            gpio.write(pin, state, (err) => {
                if (err) {
                    reject(err);
                } else {
                    this.pinStates.set(pin, state);
                    this.stats.gpioOperations++;
                    resolve();
                }
            });
        });
    }
    
    async readInput(pin) {
        if (!this.setupPins.has(pin)) {
            await this.setupPin(pin, 'in');
        }
        
        return new Promise((resolve, reject) => {
            gpio.read(pin, (err, value) => {
                if (err) {
                    reject(err);
                } else {
                    this.pinStates.set(pin, value);
                    this.stats.gpioOperations++;
                    resolve(value);
                }
            });
        });
    }
    
    async toggleOutput(pin) {
        const currentState = this.pinStates.get(pin) || false;
        await this.setOutput(pin, !currentState);
        this.log('success', `🔄 Toggled pin ${pin} to ${!currentState ? 'HIGH' : 'LOW'}`);
    }
    
    async watchPin(pin, description = '') {
        if (this.watchedPins.has(pin)) {
            this.log('warn', `Pin ${pin} is already being watched`);
            return;
        }
        
        if (!this.setupPins.has(pin)) {
            await this.setupPin(pin, 'in');
        }
        
        // Poll the pin for changes (rpi-gpio doesn't support interrupts directly)
        const watchInterval = setInterval(async () => {
            try {
                const currentState = await this.readInput(pin);
                const lastState = this.watchedPins.get(pin)?.lastState;
                
                if (lastState !== undefined && currentState !== lastState) {
                    this.stats.interrupts++;
                    this.log('interrupt', `🔔 Pin ${pin} ${description ? '(' + description + ')' : ''}: ${lastState ? 'HIGH' : 'LOW'} → ${currentState ? 'HIGH' : 'LOW'}`);
                    
                    // Send interrupt notification back to server if connected
                    if (this.connected) {
                        // Could send interrupt data back to server here if needed
                    }
                }
                
                this.watchedPins.set(pin, { 
                    interval: watchInterval, 
                    lastState: currentState,
                    description: description || `Pin ${pin}`
                });
                
            } catch (error) {
                this.log('error', `Error watching pin ${pin}: ${error.message}`);
            }
        }, 100); // Check every 100ms
        
        this.watchedPins.set(pin, { 
            interval: watchInterval, 
            lastState: undefined,
            description: description || `Pin ${pin}`
        });
        
        this.log('success', `👁️ Watching pin ${pin} ${description ? '(' + description + ')' : ''} for changes`);
    }
    
    unwatchPin(pin) {
        if (this.watchedPins.has(pin)) {
            const watchData = this.watchedPins.get(pin);
            clearInterval(watchData.interval);
            this.watchedPins.delete(pin);
            this.log('success', `👁️ Stopped watching pin ${pin}`);
        } else {
            this.log('warn', `Pin ${pin} is not being watched`);
        }
    }
    
    async blinkLED(times = 5) {
        this.log('info', `💡 Blinking LED ${times} times...`);
        
        for (let i = 0; i < times; i++) {
            await this.setOutput(18, true);
            await this.sleep(500);
            await this.setOutput(18, false);
            await this.sleep(500);
        }
        
        this.log('success', '💡 LED blink sequence completed');
    }
    
    async beepBuzzer(times = 3) {
        this.log('info', `🔊 Beeping buzzer ${times} times...`);
        
        for (let i = 0; i < times; i++) {
            await this.setOutput(24, true);
            await this.sleep(200);
            await this.setOutput(24, false);
            await this.sleep(300);
        }
        
        this.log('success', '🔊 Buzzer beep sequence completed');
    }
    
    async runTests() {
        this.log('info', '🧪 Running GPIO tests...');
        
        const tests = [
            { name: 'LED On', action: () => this.setOutput(18, true) },
            { name: 'Wait 1s', action: () => this.sleep(1000) },
            { name: 'LED Off', action: () => this.setOutput(18, false) },
            { name: 'Relay Toggle', action: () => this.toggleOutput(23) },
            { name: 'Wait 1s', action: () => this.sleep(1000) },
            { name: 'Relay Toggle', action: () => this.toggleOutput(23) },
            { name: 'Read Button', action: () => this.readInput(25) },
            { name: 'Read PIR', action: () => this.readInput(7) },
            { name: 'Buzzer Beep', action: () => this.beepBuzzer(2) }
        ];
        
        for (const test of tests) {
            try {
                this.log('info', `🧪 ${test.name}...`);
                await test.action();
                await this.sleep(200);
            } catch (error) {
                this.log('error', `❌ ${test.name} failed: ${error.message}`);
            }
        }
        
        this.log('success', '🧪 All tests completed');
    }
    
    startMonitoring(duration = 30) {
        this.log('info', `📊 Starting GPIO monitoring for ${duration} seconds...`);
        
        const monitorPins = [25, 7, 8]; // Button, PIR, Door
        const interval = setInterval(async () => {
            for (const pin of monitorPins) {
                try {
                    const state = await this.readInput(pin);
                    const pinInfo = this.setupPins.get(pin);
                    console.log(`Pin ${pin}: ${state ? 'HIGH' : 'LOW'}`);
                } catch (error) {
                    this.log('error', `Monitoring error pin ${pin}: ${error.message}`);
                }
            }
            console.log('---');
        }, 2000);
        
        setTimeout(() => {
            clearInterval(interval);
            this.log('info', '📊 Monitoring stopped');
            this.rl.prompt();
        }, duration * 1000);
    }
    
    handleLocalCommand(input) {
        if (!input) return;
        
        const parts = input.split(' ');
        const command = parts[0].toLowerCase();
        const args = parts.slice(1);
        
        try {
            if (command in this.localCommands) {
                const cmd = this.localCommands[command];
                if (typeof cmd === 'function') {
                    cmd(...args);
                } else if (typeof cmd === 'object' && args[0] in cmd) {
                    cmd[args[0]](...args.slice(1));
                } else {
                    this.log('error', `Unknown subcommand: ${args[0]}`);
                }
            } else if (command === 'exit' || command === 'quit') {
                this.shutdown();
            } else {
                this.parseDirectCommand(input);
            }
        } catch (error) {
            this.log('error', `Command error: ${error.message}`);
        }
    }
    
    parseDirectCommand(input) {
        const patterns = [
            { regex: /^pin\s+(\d+)\s+(on|high)$/i, action: (pin) => this.setOutput(parseInt(pin), true) },
            { regex: /^pin\s+(\d+)\s+(off|low)$/i, action: (pin) => this.setOutput(parseInt(pin), false) },
            { regex: /^pin\s+(\d+)\s+toggle$/i, action: (pin) => this.toggleOutput(parseInt(pin)) },
            { regex: /^read\s+(\d+)$/i, action: (pin) => this.readInput(parseInt(pin)) },
            { regex: /^(\d+)\s+(on|high)$/i, action: (pin) => this.setOutput(parseInt(pin), true) },
            { regex: /^(\d+)\s+(off|low)$/i, action: (pin) => this.setOutput(parseInt(pin), false) },
            { regex: /^(\d+)\s+toggle$/i, action: (pin) => this.toggleOutput(parseInt(pin)) },
            { regex: /^watch\s+(\d+)$/i, action: (pin) => this.watchPin(parseInt(pin)) },
            { regex: /^unwatch\s+(\d+)$/i, action: (pin) => this.unwatchPin(parseInt(pin)) }
        ];
        
        for (const pattern of patterns) {
            const match = input.match(pattern.regex);
            if (match) {
                pattern.action(match[1]);
                return;
            }
        }
        
        this.log('error', `Unknown command: ${input}. Type "help" for available commands.`);
    }
    
    showHelp() {
        console.log(chalk.yellow('\n📋 RPi GPIO SSE Client Commands:\n'));
        
        console.log(chalk.cyan('Connection:'));
        console.log('  connect          - Connect to SSE server for remote commands');
        console.log('  disconnect       - Disconnect from SSE server');
        console.log('  status           - Show GPIO and connection status');
        console.log('  stats            - Show client statistics');
        
        console.log(chalk.cyan('\nDirect GPIO Control:'));
        console.log('  on <pin>         - Turn pin ON (set HIGH)');
        console.log('  off <pin>        - Turn pin OFF (set LOW)');
        console.log('  toggle <pin>     - Toggle pin state');
        console.log('  read <pin>       - Read pin state');
        console.log('  setup <pin> <in|out> - Setup pin direction');
        console.log('  watch <pin>      - Watch pin for changes');
        console.log('  unwatch <pin>    - Stop watching pin');
        
        console.log(chalk.cyan('\nQuick Controls:'));
        console.log('  led on/off/blink - Control LED (pin 18)');
        console.log('  relay on/off/toggle - Control relay (pin 23)');
        console.log('  buzzer on/off/beep - Control buzzer (pin 24)');
        console.log('  button           - Read button (pin 25)');
        console.log('  pir              - Read PIR sensor (pin 7)');
        console.log('  door             - Read door sensor (pin 8)');
        
        console.log(chalk.cyan('\nUtility:'));
        console.log('  test             - Run GPIO test sequence');
        console.log('  monitor [sec]    - Monitor GPIO for n seconds');
        console.log('  clear            - Clear screen');
        console.log('  help             - Show this help');
        console.log('  exit/quit        - Exit application\n');
        
        console.log(chalk.yellow('Note: This client executes GPIO commands locally while listening for remote commands via SSE.\n'));
    }
    
    showStatus() {
        console.log(chalk.yellow('\n📊 System Status:\n'));
        console.log(chalk.cyan(`  SSE Connected: ${this.connected ? 'Yes' : 'No'}`));
        console.log(chalk.cyan(`  GPIO Mode: BCM`));
        console.log(chalk.cyan(`  Setup Pins: ${this.setupPins.size}`));
        console.log(chalk.cyan(`  Watched Pins: ${this.watchedPins.size}`));
        
        if (this.setupPins.size > 0) {
            console.log(chalk.cyan('\n  Configured Pins:'));
            this.setupPins.forEach((config, pin) => {
                const state = this.pinStates.get(pin);
                const stateStr = state !== undefined ? (state ? 'HIGH' : 'LOW') : 'Unknown';
                console.log(chalk.cyan(`    Pin ${pin}: ${config.direction.toUpperCase()} - ${stateStr}`));
            });
        }
        
        if (this.watchedPins.size > 0) {
            console.log(chalk.cyan('\n  Watched Pins:'));
            this.watchedPins.forEach((config, pin) => {
                console.log(chalk.cyan(`    Pin ${pin}: ${config.description}`));
            });
        }
        
        console.log();
    }
    
    showStats() {
        const uptime = (performance.now() - this.startTime) / 1000;
        
        console.log(chalk.yellow('\n📊 Client Statistics:\n'));
        console.log(chalk.cyan(`  Uptime: ${uptime.toFixed(1)}s`));
        console.log(chalk.cyan(`  SSE Connected: ${this.connected ? 'Yes' : 'No'}`));
        console.log(chalk.cyan(`  Messages Received: ${this.stats.messagesReceived}`));
        console.log(chalk.cyan(`  Commands Executed: ${this.stats.commandsExecuted}`));
        console.log(chalk.cyan(`  GPIO Operations: ${this.stats.gpioOperations}`));
        console.log(chalk.cyan(`  Interrupts Detected: ${this.stats.interrupts}`));
        console.log(chalk.cyan(`  Reconnections: ${this.stats.reconnections}`));
        console.log(chalk.cyan(`  Errors: ${this.stats.errors}`));
        console.log(chalk.cyan(`  Server URL: ${this.serverUrl}\n`));
    }
    
    log(level, message) {
        const timestamp = new Date().toLocaleTimeString();
        const colors = {
            info: chalk.blue,
            success: chalk.green,
            warn: chalk.yellow,
            error: chalk.red,
            interrupt: chalk.magenta
        };
        
        const color = colors[level] || chalk.white;
        console.log(color(`[${timestamp}] ${message}`));
    }
    
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    async shutdown() {
        this.log('info', '🛑 Shutting down RPi GPIO SSE Client...');
        
        // Stop watching pins
        for (const pin of this.watchedPins.keys()) {
            this.unwatchPin(pin);
        }
        
        // Cleanup GPIO
        try {
            gpio.destroy(() => {
                this.log('info', '🔧 GPIO cleaned up');
            });
        } catch (error) {
            this.log('error', `GPIO cleanup error: ${error.message}`);
        }
        
        // Disconnect from server
        this.disconnect();
        this.rl.close();
        
        await this.sleep(1000);
        this.log('info', '👋 Goodbye!');
        process.exit(0);
    }
    
    start() {
        console.log(chalk.yellow('🍓 Raspberry Pi GPIO SSE Client\n'));
        console.log(chalk.cyan('This client listens for SSE commands and controls GPIO directly.\n'));
        
        this.showHelp();
        
        // Auto-connect to SSE server
        this.connect();
        
        // Start interactive prompt for local commands
        this.rl.prompt();
        
        return this;
    }
}

// CLI argument parsing
function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        server: 'http://localhost:8000',
        autoConnect: true,
        verbose: false
    };
    
    for (let i = 0; i < args.length; i++) {
        switch (args[i]) {
            case '--server':
            case '-s':
                options.server = args[++i];
                break;
            case '--no-connect':
                options.autoConnect = false;
                break;
            case '--verbose':
            case '-v':
                options.verbose = true;
                break;
            case '--help':
            case '-h':
                showCLIHelp();
                process.exit(0);
                break;
        }
    }
    
    return options;
}

function showCLIHelp() {
    console.log(`
Usage: node rpi-gpio-sse-client.js [options]

Options:
  -s, --server <url>    SSE server URL (default: http://localhost:8000)
  --no-connect          Don't auto-connect on startup
  -v, --verbose         Enable verbose logging
  -h, --help            Show this help

Examples:
  node rpi-gpio-sse-client.js
  node rpi-gpio-sse-client.js --server http://192.168.1.100:8000
  node rpi-gpio-sse-client.js --no-connect

This client runs on the Raspberry Pi and:
- Listens for GPIO commands from an SSE server
- Executes commands directly on GPIO pins using rpi-gpio
- Provides local interactive GPIO control
- Monitors pins for changes and reports interrupts
`);
}

// Check dependencies
function checkDependencies() {
    const required = ['eventsource', 'rpi-gpio', 'chalk'];
    const missing = [];
    
    for (const dep of required) {
        try {
            require.resolve(dep);
        } catch (e) {
            missing.push(dep);
        }
    }
    
    if (missing.length > 0) {
        console.error(chalk.red('❌ Missing dependencies. Please install:'));
        console.error(chalk.yellow(`npm install ${missing.join(' ')}`));
        process.exit(1);
    }
}

// Main execution
if (require.main === module) {
    checkDependencies();
    
    const options = parseArgs();
    const client = new RPiGPIOSSEClient(options.server);
    
    if (!options.autoConnect) {
        client.log('info', 'Auto-connect disabled. Use "connect" command to connect to SSE server.');
    }
    
    client.start();
}

module.exports = RPiGPIOSSEClient;
