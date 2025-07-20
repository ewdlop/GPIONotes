#!/usr/bin/env node

/**
 * Node.js Server-Sent Events (SSE) Client for GPIO Control
 * Connects to Raspberry Pi GPIO SSE server and provides command interface
 */

const EventSource = require('eventsource');
const axios = require('axios');
const readline = require('readline');
const chalk = require('chalk');
const { performance } = require('perf_hooks');

class GPIOSSEClient {
    constructor(serverUrl = 'http://localhost:8000') {
        this.serverUrl = serverUrl;
        this.sseUrl = `${serverUrl}/events`;
        this.apiUrl = `${serverUrl}/api/command`;
        this.statusUrl = `${serverUrl}/api/status`;
        
        this.eventSource = null;
        this.connected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 10;
        this.reconnectDelay = 5000;
        this.startTime = performance.now();
        this.commandQueue = [];
        this.pendingCommands = new Map();
        
        // Statistics
        this.stats = {
            messagesReceived: 0,
            commandsSent: 0,
            interrupts: 0,
            reconnections: 0,
            errors: 0
        };
        
        // Setup axios defaults
        axios.defaults.timeout = 10000;
        axios.defaults.headers.post['Content-Type'] = 'application/json';
        
        // Setup readline interface for interactive commands
        this.rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout,
            prompt: chalk.cyan('gpio> ')
        });
        
        this.setupEventHandlers();
        this.setupCommands();
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
            this.handleCommand(input.trim());
            this.rl.prompt();
        });
        
        this.rl.on('close', () => {
            this.shutdown();
        });
    }
    
    setupCommands() {
        this.commands = {
            help: () => this.showHelp(),
            connect: () => this.connect(),
            disconnect: () => this.disconnect(),
            status: () => this.getServerStatus(),
            stats: () => this.showStats(),
            clear: () => console.clear(),
            
            // GPIO commands
            on: (pin) => this.setOutput(parseInt(pin), true),
            off: (pin) => this.setOutput(parseInt(pin), false),
            toggle: (pin) => this.toggleOutput(parseInt(pin)),
            read: (pin) => this.getInput(parseInt(pin)),
            gpio: () => this.getGPIOStatus(),
            ping: () => this.ping(),
            
            // Bulk operations
            bulk: () => this.sendBulkCommands(),
            test: () => this.runTests(),
            monitor: (duration) => this.startMonitoring(parseInt(duration) || 30),
            
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
            button: () => this.getInput(25),
            pir: () => this.getInput(7),
            door: () => this.getInput(8)
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
    
    handleSSEMessage(event) {
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
                    
                case 'interrupt':
                    this.stats.interrupts++;
                    this.log('interrupt', `🔔 GPIO ${data.pin} (${data.description}): ${data.state ? 'HIGH' : 'LOW'}`);
                    break;
                    
                case 'command_response':
                    this.handleCommandResponse(data);
                    break;
                    
                default:
                    this.log('info', `📨 ${JSON.stringify(data)}`);
            }
            
        } catch (error) {
            this.log('error', `Failed to parse SSE message: ${error.message}`);
            this.stats.errors++;
        }
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
    
    handleCommandResponse(data) {
        const { command_id, success, message } = data;
        
        if (this.pendingCommands.has(command_id)) {
            const { resolve, reject, startTime } = this.pendingCommands.get(command_id);
            const duration = performance.now() - startTime;
            
            this.pendingCommands.delete(command_id);
            
            if (success) {
                this.log('success', `✅ ${message} (${duration.toFixed(1)}ms)`);
                resolve(data);
            } else {
                this.log('error', `❌ ${message}`);
                reject(new Error(message));
            }
        }
    }
    
    async sendCommand(type, pin = null, state = null) {
        const command = {
            type,
            ...(pin !== null && { pin }),
            ...(state !== null && { state })
        };
        
        try {
            const startTime = performance.now();
            const response = await axios.post(this.apiUrl, command);
            const duration = performance.now() - startTime;
            
            this.stats.commandsSent++;
            
            if (response.data.success) {
                this.log('success', `✅ Command sent: ${type} ${pin ? `pin ${pin}` : ''} ${state !== null ? (state ? 'HIGH' : 'LOW') : ''} (${duration.toFixed(1)}ms)`);
                return response.data;
            } else {
                throw new Error(response.data.message || 'Unknown error');
            }
            
        } catch (error) {
            this.stats.errors++;
            const errorMsg = error.response?.data?.detail || error.message;
            this.log('error', `❌ Command failed: ${errorMsg}`);
            throw error;
        }
    }
    
    async setOutput(pin, state) {
        if (isNaN(pin) || pin < 1 || pin > 40) {
            this.log('error', 'Invalid pin number (1-40)');
            return;
        }
        return await this.sendCommand('set_output', pin, state);
    }
    
    async getInput(pin) {
        if (isNaN(pin) || pin < 1 || pin > 40) {
            this.log('error', 'Invalid pin number (1-40)');
            return;
        }
        return await this.sendCommand('get_input', pin);
    }
    
    async toggleOutput(pin) {
        if (isNaN(pin) || pin < 1 || pin > 40) {
            this.log('error', 'Invalid pin number (1-40)');
            return;
        }
        return await this.sendCommand('toggle_output', pin);
    }
    
    async getGPIOStatus() {
        return await this.sendCommand('get_status');
    }
    
    async ping() {
        const startTime = performance.now();
        try {
            await this.sendCommand('ping');
            const duration = performance.now() - startTime;
            this.log('success', `🏓 Pong received (${duration.toFixed(1)}ms)`);
        } catch (error) {
            this.log('error', `Ping failed: ${error.message}`);
        }
    }
    
    async getServerStatus() {
        try {
            const response = await axios.get(this.statusUrl);
            const status = response.data;
            
            this.log('info', `📊 Server Status:`);
            console.log(chalk.cyan(`  Connected Clients: ${status.connected_clients}`));
            console.log(chalk.cyan(`  Commands Sent: ${status.commands_sent}`));
            console.log(chalk.cyan(`  Server Time: ${status.server_time}`));
            console.log(chalk.cyan(`  Status: ${status.status}`));
            
        } catch (error) {
            this.log('error', `Failed to get server status: ${error.message}`);
        }
    }
    
    async sendBulkCommands() {
        const commands = [
            { type: 'ping' },
            { type: 'get_status' },
            { type: 'get_input', pin: 25 },
            { type: 'set_output', pin: 18, state: true },
            { type: 'set_output', pin: 18, state: false }
        ];
        
        try {
            const response = await axios.post(`${this.serverUrl}/api/commands/bulk`, commands);
            this.log('success', `📦 Bulk commands sent: ${response.data.total_commands} commands`);
            
            response.data.results.forEach((result, index) => {
                const status = result.success ? '✅' : '❌';
                this.log('info', `  ${status} Command ${index + 1}: ${JSON.stringify(result.command)}`);
            });
            
        } catch (error) {
            this.log('error', `Bulk command failed: ${error.message}`);
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
    
    async runTests() {
        this.log('info', '🧪 Running GPIO tests...');
        
        const tests = [
            () => this.ping(),
            () => this.getGPIOStatus(),
            () => this.getInput(25),
            () => this.setOutput(18, true),
            () => this.sleep(1000),
            () => this.setOutput(18, false),
            () => this.toggleOutput(23),
            () => this.sleep(1000),
            () => this.toggleOutput(23)
        ];
        
        for (const test of tests) {
            try {
                await test();
                await this.sleep(200); // Small delay between tests
            } catch (error) {
                this.log('error', `Test failed: ${error.message}`);
            }
        }
        
        this.log('success', '🧪 Tests completed');
    }
    
    startMonitoring(duration = 30) {
        this.log('info', `📊 Starting monitoring for ${duration} seconds...`);
        
        const interval = setInterval(async () => {
            try {
                await this.getGPIOStatus();
            } catch (error) {
                this.log('error', `Monitoring error: ${error.message}`);
            }
        }, 5000);
        
        setTimeout(() => {
            clearInterval(interval);
            this.log('info', '📊 Monitoring stopped');
            this.rl.prompt();
        }, duration * 1000);
    }
    
    handleCommand(input) {
        if (!input) return;
        
        const parts = input.split(' ');
        const command = parts[0].toLowerCase();
        const args = parts.slice(1);
        
        try {
            if (command in this.commands) {
                const cmd = this.commands[command];
                if (typeof cmd === 'function') {
                    cmd(...args);
                } else if (typeof cmd === 'object' && args[0] in cmd) {
                    cmd[args[0]](...args.slice(1));
                } else {
                    this.log('error', `Unknown subcommand: ${args[0]}`);
                }
            } else {
                // Try to parse as direct GPIO command
                this.parseDirectCommand(input);
            }
        } catch (error) {
            this.log('error', `Command error: ${error.message}`);
        }
    }
    
    parseDirectCommand(input) {
        // Handle direct GPIO commands like "pin 18 on", "read 25", etc.
        const patterns = [
            { regex: /^pin\s+(\d+)\s+(on|high)$/i, action: (pin) => this.setOutput(parseInt(pin), true) },
            { regex: /^pin\s+(\d+)\s+(off|low)$/i, action: (pin) => this.setOutput(parseInt(pin), false) },
            { regex: /^pin\s+(\d+)\s+toggle$/i, action: (pin) => this.toggleOutput(parseInt(pin)) },
            { regex: /^read\s+(\d+)$/i, action: (pin) => this.getInput(parseInt(pin)) },
            { regex: /^(\d+)\s+(on|high)$/i, action: (pin) => this.setOutput(parseInt(pin), true) },
            { regex: /^(\d+)\s+(off|low)$/i, action: (pin) => this.setOutput(parseInt(pin), false) },
            { regex: /^(\d+)\s+toggle$/i, action: (pin) => this.toggleOutput(parseInt(pin)) }
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
        console.log(chalk.yellow('\n📋 Available Commands:\n'));
        
        console.log(chalk.cyan('Connection:'));
        console.log('  connect          - Connect to SSE server');
        console.log('  disconnect       - Disconnect from SSE server');
        console.log('  status           - Get server status');
        console.log('  stats            - Show client statistics');
        
        console.log(chalk.cyan('\nGPIO Control:'));
        console.log('  on <pin>         - Turn pin ON (set HIGH)');
        console.log('  off <pin>        - Turn pin OFF (set LOW)');
        console.log('  toggle <pin>     - Toggle pin state');
        console.log('  read <pin>       - Read pin state');
        console.log('  gpio             - Get all GPIO status');
        console.log('  ping             - Ping the device');
        
        console.log(chalk.cyan('\nQuick Controls:'));
        console.log('  led on/off       - Control LED (pin 18)');
        console.log('  led blink [n]    - Blink LED n times');
        console.log('  relay on/off     - Control relay (pin 23)');
        console.log('  button           - Read button (pin 25)');
        console.log('  pir              - Read PIR sensor (pin 7)');
        console.log('  door             - Read door sensor (pin 8)');
        
        console.log(chalk.cyan('\nBatch Operations:'));
        console.log('  bulk             - Send bulk test commands');
        console.log('  test             - Run GPIO test sequence');
        console.log('  monitor [sec]    - Monitor GPIO for n seconds');
        
        console.log(chalk.cyan('\nDirect GPIO Syntax:'));
        console.log('  pin 18 on        - Turn pin 18 on');
        console.log('  18 off           - Turn pin 18 off');
        console.log('  read 25          - Read pin 25');
        console.log('  23 toggle        - Toggle pin 23');
        
        console.log(chalk.cyan('\nUtility:'));
        console.log('  help             - Show this help');
        console.log('  clear            - Clear screen');
        console.log('  exit/quit        - Exit application\n');
    }
    
    showStats() {
        const uptime = (performance.now() - this.startTime) / 1000;
        
        console.log(chalk.yellow('\n📊 Client Statistics:\n'));
        console.log(chalk.cyan(`  Connected: ${this.connected ? 'Yes' : 'No'}`));
        console.log(chalk.cyan(`  Uptime: ${uptime.toFixed(1)}s`));
        console.log(chalk.cyan(`  Messages Received: ${this.stats.messagesReceived}`));
        console.log(chalk.cyan(`  Commands Sent: ${this.stats.commandsSent}`));
        console.log(chalk.cyan(`  Interrupts: ${this.stats.interrupts}`));
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
        this.log('info', '🛑 Shutting down GPIO SSE Client...');
        
        this.disconnect();
        this.rl.close();
        
        // Wait a bit for cleanup
        await this.sleep(1000);
        
        this.log('info', '👋 Goodbye!');
        process.exit(0);
    }
    
    start() {
        console.log(chalk.yellow('🍓 GPIO SSE Client for Raspberry Pi\n'));
        this.showHelp();
        
        // Auto-connect
        this.connect();
        
        // Start interactive prompt
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
Usage: node gpio-sse-client.js [options]

Options:
  -s, --server <url>    SSE server URL (default: http://localhost:8000)
  --no-connect          Don't auto-connect on startup
  -v, --verbose         Enable verbose logging
  -h, --help            Show this help

Examples:
  node gpio-sse-client.js
  node gpio-sse-client.js --server http://192.168.1.100:8000
  node gpio-sse-client.js --no-connect --verbose
`);
}

// Check dependencies
function checkDependencies() {
    const required = ['eventsource', 'axios', 'chalk'];
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
    const client = new GPIOSSEClient(options.server);
    
    if (!options.autoConnect) {
        client.log('info', 'Auto-connect disabled. Use "connect" command to connect.');
    }
    
    client.start();
}

module.exports = GPIOSSEClient;
