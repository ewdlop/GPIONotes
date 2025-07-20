using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Device.Gpio;
using System.Device.Gpio.Drivers;

namespace GPIOSSEClient
{
    public enum ClientMode
    {
        Remote,  // SSE client only
        Local,   // System.Device.Gpio only
        Hybrid   // Both SSE and local GPIO
    }

    public class GPIOCommand
    {
        public string Type { get; set; }
        public int? Pin { get; set; }
        public bool? State { get; set; }
    }

    public class CommandResponse
    {
        public bool Success { get; set; }
        public string CommandId { get; set; }
        public string Message { get; set; }
        public object Data { get; set; }
    }

    public class ServerStatus
    {
        public int ConnectedClients { get; set; }
        public string ServerTime { get; set; }
        public string Status { get; set; }
        public int CommandsSent { get; set; }
    }

    public class ClientStatistics
    {
        public bool Connected { get; set; }
        public ClientMode Mode { get; set; }
        public TimeSpan Uptime { get; set; }
        public int MessagesReceived { get; set; }
        public int CommandsSent { get; set; }
        public int LocalCommands { get; set; }
        public int RemoteCommands { get; set; }
        public int Interrupts { get; set; }
        public int Reconnections { get; set; }
        public int Errors { get; set; }
    }

    public class PinConfiguration
    {
        public int Pin { get; set; }
        public PinMode Mode { get; set; }
        public PinValue InitialValue { get; set; } = PinValue.Low;
        public string Description { get; set; } = "";
        public bool EnableInterrupt { get; set; } = false;
        public PinEventTypes InterruptEvents { get; set; } = PinEventTypes.Falling | PinEventTypes.Rising;
    }

    public class HybridGPIOSSEClient : IDisposable
    {
        private readonly string _serverUrl;
        private readonly string _sseUrl;
        private readonly string _apiUrl;
        private readonly string _statusUrl;
        private readonly ClientMode _mode;
        
        // HTTP clients for remote operations
        private HttpClient _httpClient;
        private HttpClient _sseClient;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _sseTask;
        
        // GPIO controller for local operations
        private GpioController _gpioController;
        private readonly Dictionary<int, PinConfiguration> _pinConfigurations;
        private readonly Dictionary<int, PinValue> _pinStates;
        
        private bool _connected;
        private bool _gpioInitialized;
        private int _reconnectAttempts;
        private readonly int _maxReconnectAttempts = 10;
        private int _reconnectDelay = 5000;
        private readonly Stopwatch _uptime;
        
        private readonly ClientStatistics _stats;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<CommandResponse>> _pendingCommands;
        
        // Console colors
        private readonly Dictionary<string, ConsoleColor> _logColors = new()
        {
            ["info"] = ConsoleColor.Blue,
            ["success"] = ConsoleColor.Green,
            ["warn"] = ConsoleColor.Yellow,
            ["error"] = ConsoleColor.Red,
            ["interrupt"] = ConsoleColor.Magenta,
            ["local"] = ConsoleColor.Cyan,
            ["remote"] = ConsoleColor.DarkCyan
        };

        public HybridGPIOSSEClient(string serverUrl = "http://localhost:8000", ClientMode mode = ClientMode.Hybrid)
        {
            _serverUrl = serverUrl;
            _sseUrl = $"{serverUrl}/events";
            _apiUrl = $"{serverUrl}/api/command";
            _statusUrl = $"{serverUrl}/api/status";
            _mode = mode;
            
            _pinConfigurations = new Dictionary<int, PinConfiguration>();
            _pinStates = new Dictionary<int, PinValue>();
            
            if (_mode != ClientMode.Local)
            {
                _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
                _sseClient = new HttpClient { Timeout = TimeSpan.FromMilliseconds(-1) }; // No timeout for SSE
                SetupHttpClients();
            }
            
            _stats = new ClientStatistics { Mode = _mode };
            _pendingCommands = new ConcurrentDictionary<string, TaskCompletionSource<CommandResponse>>();
            _uptime = Stopwatch.StartNew();
            
            if (_mode != ClientMode.Remote)
            {
                InitializeLocalGPIO();
            }
        }

        private void SetupHttpClients()
        {
            _httpClient?.DefaultRequestHeaders.Add("Accept", "application/json");
            _sseClient?.DefaultRequestHeaders.Add("Accept", "text/event-stream");
            _sseClient?.DefaultRequestHeaders.Add("Cache-Control", "no-cache");
        }

        private void InitializeLocalGPIO()
        {
            try
            {
                // Try different GPIO drivers based on platform
                GpioDriver driver = null;
                
                if (OperatingSystem.IsLinux())
                {
                    // Try SysFs driver for Raspberry Pi
                    try
                    {
                        driver = new SysFsDriver();
                        Log("local", "Using SysFs GPIO driver for Linux");
                    }
                    catch
                    {
                        // Fallback to LibGpiodDriver if available
                        try
                        {
                            driver = new LibGpiodDriver();
                            Log("local", "Using LibGpiod GPIO driver for Linux");
                        }
                        catch
                        {
                            Log("warn", "No suitable Linux GPIO driver found, using default");
                        }
                    }
                }
                else if (OperatingSystem.IsWindows())
                {
                    // Windows IoT driver
                    Log("local", "Using Windows GPIO driver");
                }
                
                _gpioController = driver != null ? new GpioController(PinNumberingScheme.Logical, driver) 
                                                 : new GpioController(PinNumberingScheme.Logical);
                
                SetupDefaultPins();
                _gpioInitialized = true;
                
                Log("success", $"✅ Local GPIO initialized ({_gpioController.PinCount} pins available)");
            }
            catch (Exception ex)
            {
                Log("error", $"Failed to initialize local GPIO: {ex.Message}");
                Log("warn", "Local GPIO functionality disabled, using remote-only mode");
                _gpioInitialized = false;
            }
        }

        private void SetupDefaultPins()
        {
            var defaultPins = new[]
            {
                new PinConfiguration { Pin = 18, Mode = PinMode.Output, Description = "LED Output" },
                new PinConfiguration { Pin = 23, Mode = PinMode.Output, Description = "Relay Control" },
                new PinConfiguration { Pin = 24, Mode = PinMode.Output, Description = "Motor Control" },
                new PinConfiguration { Pin = 25, Mode = PinMode.InputPullUp, Description = "Button Input", EnableInterrupt = true },
                new PinConfiguration { Pin = 7, Mode = PinMode.InputPullUp, Description = "PIR Sensor", EnableInterrupt = true },
                new PinConfiguration { Pin = 8, Mode = PinMode.InputPullUp, Description = "Door Sensor", EnableInterrupt = true },
            };

            foreach (var config in defaultPins)
            {
                try
                {
                    ConfigureLocalPin(config);
                }
                catch (Exception ex)
                {
                    Log("warn", $"Failed to configure pin {config.Pin}: {ex.Message}");
                }
            }
        }

        private void ConfigureLocalPin(PinConfiguration config)
        {
            if (!_gpioInitialized) return;

            try
            {
                _pinConfigurations[config.Pin] = config;
                
                if (_gpioController.IsPinOpen(config.Pin))
                {
                    _gpioController.ClosePin(config.Pin);
                }
                
                _gpioController.OpenPin(config.Pin, config.Mode);
                
                if (config.Mode == PinMode.Output)
                {
                    _gpioController.Write(config.Pin, config.InitialValue);
                    _pinStates[config.Pin] = config.InitialValue;
                }
                
                if (config.EnableInterrupt && (config.Mode == PinMode.Input || config.Mode == PinMode.InputPullUp || config.Mode == PinMode.InputPullDown))
                {
                    _gpioController.RegisterCallbackForPinValueChangedEvent(config.Pin, config.InterruptEvents, OnPinValueChanged);
                }
                
                Log("local", $"🔧 Configured local GPIO {config.Pin} as {config.Mode} ({config.Description})");
            }
            catch (Exception ex)
            {
                Log("error", $"Failed to configure local pin {config.Pin}: {ex.Message}");
            }
        }

        private void OnPinValueChanged(object sender, PinValueChangedEventArgs e)
        {
            try
            {
                var config = _pinConfigurations.GetValueOrDefault(e.PinNumber);
                var value = e.ChangeType == PinEventTypes.Rising ? PinValue.High : PinValue.Low;
                
                _stats.Interrupts++;
                
                Log("interrupt", $"🔔 Local GPIO {e.PinNumber} ({config?.Description ?? "Unknown"}): {value}");
                
                // Handle specific interrupt callbacks
                HandleLocalInterrupt(e.PinNumber, value);
            }
            catch (Exception ex)
            {
                Log("error", $"Error handling local interrupt on pin {e.PinNumber}: {ex.Message}");
            }
        }

        private async void HandleLocalInterrupt(int pin, PinValue value)
        {
            try
            {
                switch (pin)
                {
                    case 25: // Button
                        if (value == PinValue.Low) // Button pressed (pull-up)
                        {
                            Log("interrupt", "🔘 Button pressed! Toggling LED...");
                            await ToggleOutputAsync(18);
                        }
                        break;
                        
                    case 7: // PIR Sensor
                        if (value == PinValue.High) // Motion detected
                        {
                            Log("interrupt", "👤 Motion detected! Turning on lights...");
                            await SetOutputAsync(23, true);
                            
                            // Auto turn off after 30 seconds
                            _ = Task.Delay(30000).ContinueWith(async _ =>
                            {
                                await SetOutputAsync(23, false);
                                Log("interrupt", "💡 Auto turning off lights");
                            });
                        }
                        break;
                        
                    case 8: // Door Sensor
                        if (value == PinValue.Low) // Door opened (pull-up)
                        {
                            Log("interrupt", "🚪 Door opened! Security alert...");
                            await FlashLEDAsync(5);
                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                Log("error", $"Error in interrupt handler for pin {pin}: {ex.Message}");
            }
        }

        private async Task FlashLEDAsync(int times)
        {
            for (int i = 0; i < times; i++)
            {
                await SetOutputAsync(18, true);
                await Task.Delay(200);
                await SetOutputAsync(18, false);
                await Task.Delay(200);
            }
        }

        public async Task ConnectAsync()
        {
            if (_mode == ClientMode.Local)
            {
                Log("info", "Running in local-only mode, no SSE connection needed");
                return;
            }
            
            if (_connected)
            {
                Log("warn", "Already connected to SSE server");
                return;
            }

            _cancellationTokenSource = new CancellationTokenSource();
            
            Log("remote", $"Connecting to SSE server: {_sseUrl}");
            
            _sseTask = Task.Run(async () => await SSEConnectionLoop(_cancellationTokenSource.Token));
            
            // Wait a bit to see if connection is successful
            await Task.Delay(2000);
        }

        private async Task SSEConnectionLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && _reconnectAttempts < _maxReconnectAttempts)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, _sseUrl);
                    using var response = await _sseClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        _connected = true;
                        _reconnectAttempts = 0;
                        _reconnectDelay = 5000;
                        
                        Log("success", "✅ Connected to SSE server");
                        
                        using var stream = await response.Content.ReadAsStreamAsync();
                        using var reader = new StreamReader(stream);
                        
                        await ProcessSSEStream(reader, cancellationToken);
                    }
                    else
                    {
                        throw new HttpRequestException($"SSE connection failed with status {response.StatusCode}");
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _connected = false;
                    _stats.Errors++;
                    
                    if (_reconnectAttempts < _maxReconnectAttempts)
                    {
                        _reconnectAttempts++;
                        _stats.Reconnections++;
                        
                        Log("warn", $"Connection lost. Reconnecting in {_reconnectDelay / 1000}s... ({_reconnectAttempts}/{_maxReconnectAttempts})");
                        Log("error", $"Error: {ex.Message}");
                        
                        await Task.Delay(_reconnectDelay, cancellationToken);
                        _reconnectDelay = Math.Min(_reconnectDelay * 2, 60000); // Exponential backoff
                    }
                    else
                    {
                        Log("error", "❌ Max reconnection attempts reached. Use 'connect' to retry.");
                        break;
                    }
                }
            }
        }

        private async Task ProcessSSEStream(StreamReader reader, CancellationToken cancellationToken)
        {
            string line;
            while (!cancellationToken.IsCancellationRequested && (line = await reader.ReadLineAsync()) != null)
            {
                if (line.StartsWith("data: "))
                {
                    var data = line.Substring(6);
                    await HandleSSEMessage(data);
                }
            }
        }

        private async Task HandleSSEMessage(string data)
        {
            try
            {
                using var document = JsonDocument.Parse(data);
                var root = document.RootElement;
                
                _stats.MessagesReceived++;
                
                var messageType = root.GetProperty("type").GetString();
                
                switch (messageType)
                {
                    case "connected":
                        var clientId = root.GetProperty("client_id").GetString();
                        Log("success", $"Connected with client ID: {clientId}");
                        break;
                        
                    case "heartbeat":
                        // Silent heartbeat
                        break;
                        
                    case "interrupt":
                        _stats.Interrupts++;
                        var pin = root.GetProperty("pin").GetInt32();
                        var state = root.GetProperty("state").GetBoolean();
                        var description = root.GetProperty("description").GetString();
                        Log("interrupt", $"🔔 Remote GPIO {pin} ({description}): {(state ? "HIGH" : "LOW")}");
                        break;
                        
                    case "command_response":
                        await HandleCommandResponse(root);
                        break;
                        
                    default:
                        Log("remote", $"📨 {data}");
                        break;
                }
            }
            catch (JsonException ex)
            {
                Log("error", $"Failed to parse SSE message: {ex.Message}");
                _stats.Errors++;
            }
        }

        private async Task HandleCommandResponse(JsonElement root)
        {
            var commandId = root.GetProperty("command_id").GetString();
            var success = root.GetProperty("success").GetBoolean();
            var message = root.GetProperty("message").GetString();
            
            if (_pendingCommands.TryRemove(commandId, out var tcs))
            {
                var response = new CommandResponse
                {
                    Success = success,
                    CommandId = commandId,
                    Message = message,
                    Data = root.TryGetProperty("data", out var dataElement) ? dataElement : null
                };
                
                if (success)
                {
                    Log("success", $"✅ {message}");
                    tcs.SetResult(response);
                }
                else
                {
                    Log("error", $"❌ {message}");
                    tcs.SetException(new Exception(message));
                }
            }
        }

        private async Task<CommandResponse> ExecuteCommandAsync(string type, int? pin = null, bool? state = null, bool preferLocal = true)
        {
            // Decide whether to use local or remote execution
            bool useLocal = preferLocal && _gpioInitialized && _mode != ClientMode.Remote;
            
            if (useLocal && pin.HasValue)
            {
                // Check if pin is configured locally
                useLocal = _pinConfigurations.ContainsKey(pin.Value);
            }
            
            if (useLocal)
            {
                return await ExecuteLocalCommandAsync(type, pin, state);
            }
            else if (_mode != ClientMode.Local)
            {
                return await SendRemoteCommandAsync(type, pin, state);
            }
            else
            {
                throw new InvalidOperationException("Command cannot be executed: pin not configured locally and remote mode disabled");
            }
        }

        private async Task<CommandResponse> ExecuteLocalCommandAsync(string type, int? pin = null, bool? state = null)
        {
            if (!_gpioInitialized)
            {
                throw new InvalidOperationException("Local GPIO not initialized");
            }

            try
            {
                _stats.LocalCommands++;
                var stopwatch = Stopwatch.StartNew();
                
                switch (type)
                {
                    case "set_output":
                        if (!pin.HasValue || !state.HasValue)
                            throw new ArgumentException("Pin and state required for set_output");
                        
                        var pinValue = state.Value ? PinValue.High : PinValue.Low;
                        _gpioController.Write(pin.Value, pinValue);
                        _pinStates[pin.Value] = pinValue;
                        
                        stopwatch.Stop();
                        Log("local", $"🔧 Local GPIO {pin} set to {(state.Value ? "HIGH" : "LOW")} ({stopwatch.ElapsedMilliseconds}ms)");
                        
                        return new CommandResponse
                        {
                            Success = true,
                            Message = $"Local GPIO {pin} set to {(state.Value ? "HIGH" : "LOW")}",
                            Data = new { pin = pin.Value, state = state.Value }
                        };
                        
                    case "get_input":
                        if (!pin.HasValue)
                            throw new ArgumentException("Pin required for get_input");
                        
                        var inputValue = _gpioController.Read(pin.Value);
                        stopwatch.Stop();
                        
                        Log("local", $"🔍 Local GPIO {pin} read: {inputValue} ({stopwatch.ElapsedMilliseconds}ms)");
                        
                        return new CommandResponse
                        {
                            Success = true,
                            Message = $"Local GPIO {pin} state: {inputValue}",
                            Data = new { pin = pin.Value, state = inputValue == PinValue.High }
                        };
                        
                    case "toggle_output":
                        if (!pin.HasValue)
                            throw new ArgumentException("Pin required for toggle_output");
                        
                        var currentValue = _pinStates.GetValueOrDefault(pin.Value, PinValue.Low);
                        var newValue = currentValue == PinValue.High ? PinValue.Low : PinValue.High;
                        
                        _gpioController.Write(pin.Value, newValue);
                        _pinStates[pin.Value] = newValue;
                        
                        stopwatch.Stop();
                        Log("local", $"🔄 Local GPIO {pin} toggled to {newValue} ({stopwatch.ElapsedMilliseconds}ms)");
                        
                        return new CommandResponse
                        {
                            Success = true,
                            Message = $"Local GPIO {pin} toggled to {newValue}",
                            Data = new { pin = pin.Value, state = newValue == PinValue.High }
                        };
                        
                    case "get_status":
                        var status = GetLocalGPIOStatus();
                        stopwatch.Stop();
                        
                        Log("local", $"📊 Local GPIO status retrieved ({stopwatch.ElapsedMilliseconds}ms)");
                        
                        return new CommandResponse
                        {
                            Success = true,
                            Message = "Local GPIO status retrieved",
                            Data = status
                        };
                        
                    case "ping":
                        stopwatch.Stop();
                        Log("local", $"🏓 Local pong ({stopwatch.ElapsedMilliseconds}ms)");
                        
                        return new CommandResponse
                        {
                            Success = true,
                            Message = "Local pong",
                            Data = new { latency = stopwatch.ElapsedMilliseconds }
                        };
                        
                    default:
                        throw new ArgumentException($"Unknown local command type: {type}");
                }
            }
            catch (Exception ex)
            {
                _stats.Errors++;
                Log("error", $"❌ Local command failed: {ex.Message}");
                throw;
            }
        }

        private object GetLocalGPIOStatus()
        {
            var outputs = new Dictionary<string, object>();
            var inputs = new Dictionary<string, object>();
            
            foreach (var config in _pinConfigurations.Values)
            {
                try
                {
                    var pinData = new
                    {
                        pin = config.Pin,
                        description = config.Description,
                        state = config.Mode == PinMode.Output 
                            ? _pinStates.GetValueOrDefault(config.Pin, PinValue.Low) == PinValue.High
                            : _gpioController.Read(config.Pin) == PinValue.High
                    };
                    
                    if (config.Mode == PinMode.Output)
                    {
                        outputs[config.Pin.ToString()] = pinData;
                    }
                    else
                    {
                        inputs[config.Pin.ToString()] = pinData;
                    }
                }
                catch (Exception ex)
                {
                    var errorData = new
                    {
                        pin = config.Pin,
                        description = config.Description,
                        error = ex.Message
                    };
                    
                    if (config.Mode == PinMode.Output)
                    {
                        outputs[config.Pin.ToString()] = errorData;
                    }
                    else
                    {
                        inputs[config.Pin.ToString()] = errorData;
                    }
                }
            }
            
            return new
            {
                outputs,
                inputs,
                timestamp = DateTime.Now.ToString("O"),
                mode = "local",
                driver = _gpioController?.GetType().Name ?? "Unknown"
            };
        }

        private async Task<CommandResponse> SendRemoteCommandAsync(string type, int? pin = null, bool? state = null)
        {
            var command = new GPIOCommand
            {
                Type = type,
                Pin = pin,
                State = state
            };

            var json = JsonSerializer.Serialize(command);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                var response = await _httpClient.PostAsync(_apiUrl, content);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                stopwatch.Stop();
                _stats.RemoteCommands++;
                
                if (response.IsSuccessStatusCode)
                {
                    var result = JsonSerializer.Deserialize<CommandResponse>(responseContent);
                    var pinInfo = pin.HasValue ? $"pin {pin}" : "";
                    var stateInfo = state.HasValue ? (state.Value ? "HIGH" : "LOW") : "";
                    
                    Log("remote", $"✅ Remote command sent: {type} {pinInfo} {stateInfo} ({stopwatch.ElapsedMilliseconds}ms)");
                    return result;
                }
                else
                {
                    throw new HttpRequestException($"Remote command failed with status {response.StatusCode}: {responseContent}");
                }
            }
            catch (Exception ex)
            {
                _stats.Errors++;
                Log("error", $"❌ Remote command failed: {ex.Message}");
                throw;
            }
        }

        public async Task<CommandResponse> SetOutputAsync(int pin, bool state, bool preferLocal = true)
        {
            if (pin < 1 || pin > 40)
            {
                throw new ArgumentException("Invalid pin number (1-40)");
            }
            return await ExecuteCommandAsync("set_output", pin, state, preferLocal);
        }

        public async Task<CommandResponse> GetInputAsync(int pin, bool preferLocal = true)
        {
            if (pin < 1 || pin > 40)
            {
                throw new ArgumentException("Invalid pin number (1-40)");
            }
            return await ExecuteCommandAsync("get_input", pin, preferLocal: preferLocal);
        }

        public async Task<CommandResponse> ToggleOutputAsync(int pin, bool preferLocal = true)
        {
            if (pin < 1 || pin > 40)
            {
                throw new ArgumentException("Invalid pin number (1-40)");
            }
            return await ExecuteCommandAsync("toggle_output", pin, preferLocal: preferLocal);
        }

        public async Task<CommandResponse> GetGPIOStatusAsync(bool preferLocal = true)
        {
            return await ExecuteCommandAsync("get_status", preferLocal: preferLocal);
        }

        public async Task<CommandResponse> PingAsync(bool preferLocal = true)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var result = await ExecuteCommandAsync("ping", preferLocal: preferLocal);
                stopwatch.Stop();
                Log("success", $"🏓 Pong received ({stopwatch.ElapsedMilliseconds}ms)");
                return result;
            }
            catch (Exception ex)
            {
                Log("error", $"Ping failed: {ex.Message}");
                throw;
            }
        }

        public async Task<ServerStatus> GetServerStatusAsync()
        {
            if (_mode == ClientMode.Local)
            {
                throw new InvalidOperationException("Server status not available in local-only mode");
            }
            
            try
            {
                var response = await _httpClient.GetAsync(_statusUrl);
                var content = await response.Content.ReadAsStringAsync();
                
                if (response.IsSuccessStatusCode)
                {
                    var status = JsonSerializer.Deserialize<ServerStatus>(content);
                    
                    Log("remote", "📊 Server Status:");
                    Console.WriteLine($"  Connected Clients: {status.ConnectedClients}");
                    Console.WriteLine($"  Commands Sent: {status.CommandsSent}");
                    Console.WriteLine($"  Server Time: {status.ServerTime}");
                    Console.WriteLine($"  Status: {status.Status}");
                    
                    return status;
                }
                else
                {
                    throw new HttpRequestException($"Failed to get server status: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Log("error", $"Failed to get server status: {ex.Message}");
                throw;
            }
        }

        public async Task SendBulkCommandsAsync()
        {
            if (_mode == ClientMode.Local)
            {
                // Execute bulk commands locally
                Log("local", "📦 Executing bulk commands locally...");
                
                var localCommands = new[]
                {
                    () => PingAsync(true),
                    () => GetGPIOStatusAsync(true),
                    () => GetInputAsync(25, true),
                    () => SetOutputAsync(18, true, true),
                    () => Task.Delay(500),
                    () => SetOutputAsync(18, false, true)
                };
                
                foreach (var command in localCommands)
                {
                    try
                    {
                        await command();
                    }
                    catch (Exception ex)
                    {
                        Log("error", $"Bulk command failed: {ex.Message}");
                    }
                }
                
                Log("success", $"📦 Local bulk commands completed");
                return;
            }

            var commands = new[]
            {
                new GPIOCommand { Type = "ping" },
                new GPIOCommand { Type = "get_status" },
                new GPIOCommand { Type = "get_input", Pin = 25 },
                new GPIOCommand { Type = "set_output", Pin = 18, State = true },
                new GPIOCommand { Type = "set_output", Pin = 18, State = false }
            };

            var json = JsonSerializer.Serialize(commands);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            try
            {
                var response = await _httpClient.PostAsync($"{_serverUrl}/api/commands/bulk", content);
                var responseContent = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    using var document = JsonDocument.Parse(responseContent);
                    var root = document.RootElement;
                    var totalCommands = root.GetProperty("total_commands").GetInt32();
                    
                    Log("success", $"📦 Bulk commands sent: {totalCommands} commands");
                    
                    var results = root.GetProperty("results").EnumerateArray();
                    var index = 1;
                    foreach (var result in results)
                    {
                        var success = result.GetProperty("success").GetBoolean();
                        var command = result.Getusing System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.CommandLine;
using System.CommandLine.Invocation;

namespace GPIOSSEClient
{
    public class GPIOCommand
    {
        public string Type { get; set; }
        public int? Pin { get; set; }
        public bool? State { get; set; }
    }

    public class CommandResponse
    {
        public bool Success { get; set; }
        public string CommandId { get; set; }
        public string Message { get; set; }
        public object Data { get; set; }
    }

    public class ServerStatus
    {
        public int ConnectedClients { get; set; }
        public string ServerTime { get; set; }
        public string Status { get; set; }
        public int CommandsSent { get; set; }
    }

    public class ClientStatistics
    {
        public bool Connected { get; set; }
        public TimeSpan Uptime { get; set; }
        public int MessagesReceived { get; set; }
        public int CommandsSent { get; set; }
        public int Interrupts { get; set; }
        public int Reconnections { get; set; }
        public int Errors { get; set; }
    }

    public class GPIOSSEClient : IDisposable
    {
        private readonly string _serverUrl;
        private readonly string _sseUrl;
        private readonly string _apiUrl;
        private readonly string _statusUrl;
        
        private HttpClient _httpClient;
        private HttpClient _sseClient;
        private CancellationTokenSource _cancellationTokenSource;
        private Task _sseTask;
        
        private bool _connected;
        private int _reconnectAttempts;
        private readonly int _maxReconnectAttempts = 10;
        private int _reconnectDelay = 5000;
        private readonly Stopwatch _uptime;
        
        private readonly ClientStatistics _stats;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<CommandResponse>> _pendingCommands;
        
        // Console colors
        private readonly Dictionary<string, ConsoleColor> _logColors = new()
        {
            ["info"] = ConsoleColor.Blue,
            ["success"] = ConsoleColor.Green,
            ["warn"] = ConsoleColor.Yellow,
            ["error"] = ConsoleColor.Red,
            ["interrupt"] = ConsoleColor.Magenta
        };

        public GPIOSSEClient(string serverUrl = "http://localhost:8000")
        {
            _serverUrl = serverUrl;
            _sseUrl = $"{serverUrl}/events";
            _apiUrl = $"{serverUrl}/api/command";
            _statusUrl = $"{serverUrl}/api/status";
            
            _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            _sseClient = new HttpClient { Timeout = TimeSpan.FromMilliseconds(-1) }; // No timeout for SSE
            
            _stats = new ClientStatistics();
            _pendingCommands = new ConcurrentDictionary<string, TaskCompletionSource<CommandResponse>>();
            _uptime = Stopwatch.StartNew();
            
            SetupHttpClients();
        }

        private void SetupHttpClients()
        {
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            _sseClient.DefaultRequestHeaders.Add("Accept", "text/event-stream");
            _sseClient.DefaultRequestHeaders.Add("Cache-Control", "no-cache");
        }

        public async Task ConnectAsync()
        {
            if (_connected)
            {
                Log("warn", "Already connected to SSE server");
                return;
            }

            _cancellationTokenSource = new CancellationTokenSource();
            
            Log("info", $"Connecting to SSE server: {_sseUrl}");
            
            _sseTask = Task.Run(async () => await SSEConnectionLoop(_cancellationTokenSource.Token));
            
            // Wait a bit to see if connection is successful
            await Task.Delay(2000);
        }

        private async Task SSEConnectionLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && _reconnectAttempts < _maxReconnectAttempts)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, _sseUrl);
                    using var response = await _sseClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        _connected = true;
                        _reconnectAttempts = 0;
                        _reconnectDelay = 5000;
                        
                        Log("success", "✅ Connected to SSE server");
                        
                        using var stream = await response.Content.ReadAsStreamAsync();
                        using var reader = new StreamReader(stream);
                        
                        await ProcessSSEStream(reader, cancellationToken);
                    }
                    else
                    {
                        throw new HttpRequestException($"SSE connection failed with status {response.StatusCode}");
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _connected = false;
                    _stats.Errors++;
                    
                    if (_reconnectAttempts < _maxReconnectAttempts)
                    {
                        _reconnectAttempts++;
                        _stats.Reconnections++;
                        
                        Log("warn", $"Connection lost. Reconnecting in {_reconnectDelay / 1000}s... ({_reconnectAttempts}/{_maxReconnectAttempts})");
                        Log("error", $"Error: {ex.Message}");
                        
                        await Task.Delay(_reconnectDelay, cancellationToken);
                        _reconnectDelay = Math.Min(_reconnectDelay * 2, 60000); // Exponential backoff
                    }
                    else
                    {
                        Log("error", "❌ Max reconnection attempts reached. Use 'connect' to retry.");
                        break;
                    }
                }
            }
        }

        private async Task ProcessSSEStream(StreamReader reader, CancellationToken cancellationToken)
        {
            string line;
            while (!cancellationToken.IsCancellationRequested && (line = await reader.ReadLineAsync()) != null)
            {
                if (line.StartsWith("data: "))
                {
                    var data = line.Substring(6);
                    await HandleSSEMessage(data);
                }
            }
        }

        private async Task HandleSSEMessage(string data)
        {
            try
            {
                using var document = JsonDocument.Parse(data);
                var root = document.RootElement;
                
                _stats.MessagesReceived++;
                
                var messageType = root.GetProperty("type").GetString();
                
                switch (messageType)
                {
                    case "connected":
                        var clientId = root.GetProperty("client_id").GetString();
                        Log("success", $"Connected with client ID: {clientId}");
                        break;
                        
                    case "heartbeat":
                        // Silent heartbeat
                        break;
                        
                    case "interrupt":
                        _stats.Interrupts++;
                        var pin = root.GetProperty("pin").GetInt32();
                        var state = root.GetProperty("state").GetBoolean();
                        var description = root.GetProperty("description").GetString();
                        Log("interrupt", $"🔔 GPIO {pin} ({description}): {(state ? "HIGH" : "LOW")}");
                        break;
                        
                    case "command_response":
                        await HandleCommandResponse(root);
                        break;
                        
                    default:
                        Log("info", $"📨 {data}");
                        break;
                }
            }
            catch (JsonException ex)
            {
                Log("error", $"Failed to parse SSE message: {ex.Message}");
                _stats.Errors++;
            }
        }

        private async Task HandleCommandResponse(JsonElement root)
        {
            var commandId = root.GetProperty("command_id").GetString();
            var success = root.GetProperty("success").GetBoolean();
            var message = root.GetProperty("message").GetString();
            
            if (_pendingCommands.TryRemove(commandId, out var tcs))
            {
                var response = new CommandResponse
                {
                    Success = success,
                    CommandId = commandId,
                    Message = message,
                    Data = root.TryGetProperty("data", out var dataElement) ? dataElement : null
                };
                
                if (success)
                {
                    Log("success", $"✅ {message}");
                    tcs.SetResult(response);
                }
                else
                {
                    Log("error", $"❌ {message}");
                    tcs.SetException(new Exception(message));
                }
            }
        }

        public async Task<CommandResponse> SendCommandAsync(string type, int? pin = null, bool? state = null)
        {
            var command = new GPIOCommand
            {
                Type = type,
                Pin = pin,
                State = state
            };

            var json = JsonSerializer.Serialize(command);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                var response = await _httpClient.PostAsync(_apiUrl, content);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                stopwatch.Stop();
                _stats.CommandsSent++;
                
                if (response.IsSuccessStatusCode)
                {
                    var result = JsonSerializer.Deserialize<CommandResponse>(responseContent);
                    var pinInfo = pin.HasValue ? $"pin {pin}" : "";
                    var stateInfo = state.HasValue ? (state.Value ? "HIGH" : "LOW") : "";
                    
                    Log("success", $"✅ Command sent: {type} {pinInfo} {stateInfo} ({stopwatch.ElapsedMilliseconds}ms)");
                    return result;
                }
                else
                {
                    throw new HttpRequestException($"Command failed with status {response.StatusCode}: {responseContent}");
                }
            }
            catch (Exception ex)
            {
                _stats.Errors++;
                Log("error", $"❌ Command failed: {ex.Message}");
                throw;
            }
        }

        public async Task<CommandResponse> SetOutputAsync(int pin, bool state)
        {
            if (pin < 1 || pin > 40)
            {
                throw new ArgumentException("Invalid pin number (1-40)");
            }
            return await SendCommandAsync("set_output", pin, state);
        }

        public async Task<CommandResponse> GetInputAsync(int pin)
        {
            if (pin < 1 || pin > 40)
            {
                throw new ArgumentException("Invalid pin number (1-40)");
            }
            return await SendCommandAsync("get_input", pin);
        }

        public async Task<CommandResponse> ToggleOutputAsync(int pin)
        {
            if (pin < 1 || pin > 40)
            {
                throw new ArgumentException("Invalid pin number (1-40)");
            }
            return await SendCommandAsync("toggle_output", pin);
        }

        public async Task<CommandResponse> GetGPIOStatusAsync()
        {
            return await SendCommandAsync("get_status");
        }

        public async Task<CommandResponse> PingAsync()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var result = await SendCommandAsync("ping");
                stopwatch.Stop();
                Log("success", $"🏓 Pong received ({stopwatch.ElapsedMilliseconds}ms)");
                return result;
            }
            catch (Exception ex)
            {
                Log("error", $"Ping failed: {ex.Message}");
                throw;
            }
        }

        public async Task<ServerStatus> GetServerStatusAsync()
        {
            try
            {
                var response = await _httpClient.GetAsync(_statusUrl);
                var content = await response.Content.ReadAsStringAsync();
                
                if (response.IsSuccessStatusCode)
                {
                    var status = JsonSerializer.Deserialize<ServerStatus>(content);
                    
                    Log("info", "📊 Server Status:");
                    Console.WriteLine($"  Connected Clients: {status.ConnectedClients}");
                    Console.WriteLine($"  Commands Sent: {status.CommandsSent}");
                    Console.WriteLine($"  Server Time: {status.ServerTime}");
                    Console.WriteLine($"  Status: {status.Status}");
                    
                    return status;
                }
                else
                {
                    throw new HttpRequestException($"Failed to get server status: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Log("error", $"Failed to get server status: {ex.Message}");
                throw;
            }
        }

        public async Task SendBulkCommandsAsync()
        {
            var commands = new[]
            {
                new GPIOCommand { Type = "ping" },
                new GPIOCommand { Type = "get_status" },
                new GPIOCommand { Type = "get_input", Pin = 25 },
                new GPIOCommand { Type = "set_output", Pin = 18, State = true },
                new GPIOCommand { Type = "set_output", Pin = 18, State = false }
            };

            var json = JsonSerializer.Serialize(commands);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            try
            {
                var response = await _httpClient.PostAsync($"{_serverUrl}/api/commands/bulk", content);
                var responseContent = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    using var document = JsonDocument.Parse(responseContent);
                    var root = document.RootElement;
                    var totalCommands = root.GetProperty("total_commands").GetInt32();
                    
                    Log("success", $"📦 Bulk commands sent: {totalCommands} commands");
                    
                    var results = root.GetProperty("results").EnumerateArray();
                    var index = 1;
                    foreach (var result in results)
                    {
                        var success = result.GetProperty("success").GetBoolean();
                        var command = result.GetProperty("command");
                        var status = success ? "✅" : "❌";
                        Log("info", $"  {status} Command {index}: {command}");
                        index++;
                    }
                }
                else
                {
                    throw new HttpRequestException($"Bulk command failed: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Log("error", $"Bulk command failed: {ex.Message}");
                throw;
            }
        }

        public async Task BlinkLEDAsync(int times = 5)
        {
            Log("info", $"💡 Blinking LED {times} times...");
            
            for (int i = 0; i < times; i++)
            {
                await SetOutputAsync(18, true);
                await Task.Delay(500);
                await SetOutputAsync(18, false);
                await Task.Delay(500);
            }
            
            Log("success", "💡 LED blink sequence completed");
        }

        public async Task RunTestsAsync()
        {
            Log("info", "🧪 Running GPIO tests...");
            
            var tests = new Func<Task>[]
            {
                () => PingAsync(),
                () => GetGPIOStatusAsync(),
                () => GetInputAsync(25),
                () => SetOutputAsync(18, true),
                () => Task.Delay(1000),
                () => SetOutputAsync(18, false),
                () => ToggleOutputAsync(23),
                () => Task.Delay(1000),
                () => ToggleOutputAsync(23)
            };
            
            foreach (var test in tests)
            {
                try
                {
                    await test();
                    await Task.Delay(200); // Small delay between tests
                }
                catch (Exception ex)
                {
                    Log("error", $"Test failed: {ex.Message}");
                }
            }
            
            Log("success", "🧪 Tests completed");
        }

        public async Task StartMonitoringAsync(int durationSeconds = 30)
        {
            Log("info", $"📊 Starting monitoring for {durationSeconds} seconds...");
            
            var cancellationToken = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
            
            try
            {
                while (!cancellationToken.Token.IsCancellationRequested)
                {
                    try
                    {
                        await GetGPIOStatusAsync();
                        await Task.Delay(5000, cancellationToken.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Log("error", $"Monitoring error: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when monitoring time expires
            }
            
            Log("info", "📊 Monitoring stopped");
        }

        public void ShowStats()
        {
            _stats.Connected = _connected;
            _stats.Uptime = _uptime.Elapsed;
            
            Console.WriteLine();
            Log("info", "📊 Client Statistics:");
            Console.WriteLine($"  Connected: {(_stats.Connected ? "Yes" : "No")}");
            Console.WriteLine($"  Uptime: {_stats.Uptime.TotalSeconds:F1}s");
            Console.WriteLine($"  Messages Received: {_stats.MessagesReceived}");
            Console.WriteLine($"  Commands Sent: {_stats.CommandsSent}");
            Console.WriteLine($"  Interrupts: {_stats.Interrupts}");
            Console.WriteLine($"  Reconnections: {_stats.Reconnections}");
            Console.WriteLine($"  Errors: {_stats.Errors}");
            Console.WriteLine($"  Server URL: {_serverUrl}");
            Console.WriteLine();
        }

        public void Disconnect()
        {
            _connected = false;
            _cancellationTokenSource?.Cancel();
            
            Log("info", "🔴 Disconnected from SSE server");
        }

        private void Log(string level, string message)
        {
            var timestamp = DateTime.Now.ToString("HH:mm:ss");
            var color = _logColors.GetValueOrDefault(level, ConsoleColor.White);
            
            var originalColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine($"[{timestamp}] {message}");
            Console.ForegroundColor = originalColor;
        }

        private void ShowHelp()
        {
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("📋 Available Commands:");
            Console.ForegroundColor = ConsoleColor.Cyan;
            
            Console.WriteLine("\nConnection:");
            Console.WriteLine("  connect          - Connect to SSE server");
            Console.WriteLine("  disconnect       - Disconnect from SSE server");
            Console.WriteLine("  status           - Get server status");
            Console.WriteLine("  stats            - Show client statistics");
            
            Console.WriteLine("\nGPIO Control:");
            Console.WriteLine("  on <pin>         - Turn pin ON (set HIGH)");
            Console.WriteLine("  off <pin>        - Turn pin OFF (set LOW)");
            Console.WriteLine("  toggle <pin>     - Toggle pin state");
            Console.WriteLine("  read <pin>       - Read pin state");
            Console.WriteLine("  gpio             - Get all GPIO status");
            Console.WriteLine("  ping             - Ping the device");
            
            Console.WriteLine("\nQuick Controls:");
            Console.WriteLine("  led on/off       - Control LED (pin 18)");
            Console.WriteLine("  led blink [n]    - Blink LED n times");
            Console.WriteLine("  relay on/off     - Control relay (pin 23)");
            Console.WriteLine("  button           - Read button (pin 25)");
            Console.WriteLine("  pir              - Read PIR sensor (pin 7)");
            Console.WriteLine("  door             - Read door sensor (pin 8)");
            
            Console.WriteLine("\nBatch Operations:");
            Console.WriteLine("  bulk             - Send bulk test commands");
            Console.WriteLine("  test             - Run GPIO test sequence");
            Console.WriteLine("  monitor [sec]    - Monitor GPIO for n seconds");
            
            Console.WriteLine("\nUtility:");
            Console.WriteLine("  help             - Show this help");
            Console.WriteLine("  clear            - Clear screen");
            Console.WriteLine("  exit/quit        - Exit application");
            
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine();
        }

        public async Task StartInteractiveMode()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("🍓 .NET GPIO SSE Client for Raspberry Pi");
            Console.ForegroundColor = ConsoleColor.White;
            
            ShowHelp();
            
            // Auto-connect
            await ConnectAsync();
            
            while (true)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.Write("gpio> ");
                Console.ForegroundColor = ConsoleColor.White;
                
                var input = Console.ReadLine()?.Trim();
                if (string.IsNullOrEmpty(input)) continue;
                
                var parts = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var command = parts[0].ToLower();
                
                try
                {
                    switch (command)
                    {
                        case "help":
                            ShowHelp();
                            break;
                            
                        case "connect":
                            await ConnectAsync();
                            break;
                            
                        case "disconnect":
                            Disconnect();
                            break;
                            
                        case "status":
                            await GetServerStatusAsync();
                            break;
                            
                        case "stats":
                            ShowStats();
                            break;
                            
                        case "clear":
                            Console.Clear();
                            break;
                            
                        case "on":
                            if (parts.Length > 1 && int.TryParse(parts[1], out var onPin))
                                await SetOutputAsync(onPin, true);
                            else
                                Log("error", "Usage: on <pin>");
                            break;
                            
                        case "off":
                            if (parts.Length > 1 && int.TryParse(parts[1], out var offPin))
                                await SetOutputAsync(offPin, false);
                            else
                                Log("error", "Usage: off <pin>");
                            break;
                            
                        case "toggle":
                            if (parts.Length > 1 && int.TryParse(parts[1], out var togglePin))
                                await ToggleOutputAsync(togglePin);
                            else
                                Log("error", "Usage: toggle <pin>");
                            break;
                            
                        case "read":
                            if (parts.Length > 1 && int.TryParse(parts[1], out var readPin))
                                await GetInputAsync(readPin);
                            else
                                Log("error", "Usage: read <pin>");
                            break;
                            
                        case "gpio":
                            await GetGPIOStatusAsync();
                            break;
                            
                        case "ping":
                            await PingAsync();
                            break;
                            
                        case "led":
                            if (parts.Length > 1)
                            {
                                switch (parts[1].ToLower())
                                {
                                    case "on":
                                        await SetOutputAsync(18, true);
                                        break;
                                    case "off":
                                        await SetOutputAsync(18, false);
                                        break;
                                    case "blink":
                                        var times = parts.Length > 2 && int.TryParse(parts[2], out var t) ? t : 5;
                                        await BlinkLEDAsync(times);
                                        break;
                                    default:
                                        Log("error", "Usage: led on/off/blink [times]");
                                        break;
                                }
                            }
                            else
                            {
                                Log("error", "Usage: led on/off/blink [times]");
                            }
                            break;
                            
                        case "relay":
                            if (parts.Length > 1)
                            {
                                switch (parts[1].ToLower())
                                {
                                    case "on":
                                        await SetOutputAsync(23, true);
                                        break;
                                    case "off":
                                        await SetOutputAsync(23, false);
                                        break;
                                    case "toggle":
                                        await ToggleOutputAsync(23);
                                        break;
                                    default:
                                        Log("error", "Usage: relay on/off/toggle");
                                        break;
                                }
                            }
                            else
                            {
                                Log("error", "Usage: relay on/off/toggle");
                            }
                            break;
                            
                        case "button":
                            await GetInputAsync(25);
                            break;
                            
                        case "pir":
                            await GetInputAsync(7);
                            break;
                            
                        case "door":
                            await GetInputAsync(8);
                            break;
                            
                        case "bulk":
                            await SendBulkCommandsAsync();
                            break;
                            
                        case "test":
                            await RunTestsAsync();
                            break;
                            
                        case "monitor":
                            var duration = parts.Length > 1 && int.TryParse(parts[1], out var d) ? d : 30;
                            await StartMonitoringAsync(duration);
                            break;
                            
                        case "exit":
                        case "quit":
                            await ShutdownAsync();
                            return;
                            
                        default:
                            // Try to parse as direct GPIO command
                            if (int.TryParse(parts[0], out var pin) && parts.Length > 1)
                            {
                                switch (parts[1].ToLower())
                                {
                                    case "on":
                                    case "high":
                                        await SetOutputAsync(pin, true);
                                        break;
                                    case "off":
                                    case "low":
                                        await SetOutputAsync(pin, false);
                                        break;
                                    case "toggle":
                                        await ToggleOutputAsync(pin);
                                        break;
                                    default:
                                        Log("error", $"Unknown command: {input}. Type 'help' for available commands.");
                                        break;
                                }
                            }
                            else
                            {
                                Log("error", $"Unknown command: {input}. Type 'help' for available commands.");
                            }
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Log("error", $"Command error: {ex.Message}");
                }
            }
        }

        public async Task ShutdownAsync()
        {
            Log("info", "🛑 Shutting down GPIO SSE Client...");
            
            Disconnect();
            
            // Wait a bit for cleanup
            await Task.Delay(1000);
            
            Log("info", "👋 Goodbye!");
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Cancel();
            _sseTask?.Wait(TimeSpan.FromSeconds(5));
            
            _httpClient?.Dispose();
            _sseClient?.Dispose();
            _cancellationTokenSource?.Dispose();
        }
    }

    class Program
    {
        static async Task<int> Main(string[] args)
        {
            var serverOption = new Option<string>(
                "--server",
                getDefaultValue: () => "http://localhost:8000",
                description: "SSE server URL");
            serverOption.AddAlias("-s");

            var noConnectOption = new Option<bool>(
                "--no-connect",
                getDefaultValue: () => false,
                description: "Don't auto-connect on startup");

            var verboseOption = new Option<bool>(
                "--verbose",
                getDefaultValue: () => false,
                description: "Enable verbose logging");
            verboseOption.AddAlias("-v");

            var rootCommand = new RootCommand("🍓 .NET GPIO SSE Client for Raspberry Pi")
            {
                serverOption,
                noConnectOption,
                verboseOption
            };

            rootCommand.SetHandler(async (server, noConnect, verbose) =>
            {
                try
                {
                    using var client = new GPIOSSEClient(server);
                    
                    if (verbose)
                    {
                        Console.WriteLine($"Server URL: {server}");
                        Console.WriteLine($"Auto-connect: {!noConnect}");
                    }
                    
                    // Setup Ctrl+C handler
                    Console.CancelKeyPress += async (s, e) =>
                    {
                        e.Cancel = true;
                        await client.ShutdownAsync();
                        Environment.Exit(0);
                    };
                    
                    await client.StartInteractiveMode();
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Application failed: {ex.Message}");
                    Console.ForegroundColor = ConsoleColor.White;
                    Environment.Exit(1);
                }
            }, serverOption, noConnectOption, verboseOption);

            return await rootCommand.InvokeAsync(args);
        }
    }
}
