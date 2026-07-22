using System.Diagnostics;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;

if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("MAR Productivy Analytics requiere Windows 10 u 11.");

var configPath = Path.Combine(AppContext.BaseDirectory, "agent.json");
if (!File.Exists(configPath)) {
    Console.Error.WriteLine($"Falta la configuración: {configPath}");
    return 2;
}

var config = JsonSerializer.Deserialize<AgentConfig>(await File.ReadAllTextAsync(configPath), JsonOptions.Default)
             ?? throw new InvalidOperationException("La configuración no es válida.");
const string agentVersion = "2.0.0";
var agentData = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MAR Productivy Analytics", "Agent");
Directory.CreateDirectory(agentData);
var spoolPath = Path.Combine(agentData, "spool.jsonl");
var logPath = Path.Combine(agentData, "agent.log");
using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
var pending = new List<ActivityEvent>();
ForegroundSample? previous = null;
var previousAt = DateTimeOffset.UtcNow;
var nextSync = DateTimeOffset.UtcNow;
var nextHeartbeat = DateTimeOffset.MinValue;

async Task Heartbeat(string? error = null) {
    try {
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(new Uri(config.ServerUrl.TrimEnd('/') + "/"), "api/heartbeat"));
        request.Headers.Add("x-device-id", config.DeviceId); request.Headers.Add("x-device-key", config.DeviceKey); request.Headers.Add("x-agent-version", agentVersion);
        if (!string.IsNullOrWhiteSpace(error)) request.Headers.TryAddWithoutValidation("x-agent-error", error);
        using var response = await client.SendAsync(request); response.EnsureSuccessStatusCode();
        await File.AppendAllTextAsync(logPath, $"{DateTimeOffset.Now:u} Conexión correcta con {config.ServerUrl}{Environment.NewLine}");
    } catch (Exception heartbeatError) { await File.AppendAllTextAsync(logPath, $"{DateTimeOffset.Now:u} Error de conexión: {heartbeatError.Message}{Environment.NewLine}"); }
}

Console.WriteLine("MAR Productivy Analytics Agent iniciado. Presiona Ctrl+C para salir.");
while (true) {
    var now = DateTimeOffset.UtcNow;
    if (now >= nextHeartbeat) { await Heartbeat(); nextHeartbeat=now.AddMinutes(1); }
    var current = ForegroundReader.Read();
    if (previous is not null && current is not null) {
        var elapsed = Math.Clamp((int)(now - previousAt).TotalSeconds, 1, 60);
        var idle = Math.Min(elapsed, ForegroundReader.IdleSeconds());
        pending.Add(new ActivityEvent(Guid.NewGuid().ToString("N"), previous.AppName, config.CollectWindowTitles ? previous.WindowTitle : "", previousAt, now, elapsed, idle));
    }
    previous = current;
    previousAt = now;

    if (now >= nextSync && pending.Count > 0) {
        var batch = pending.ToArray();
        try {
            using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(new Uri(config.ServerUrl.TrimEnd('/') + "/"), "api/ingest"));
            request.Headers.Add("x-device-id", config.DeviceId);
            request.Headers.Add("x-device-key", config.DeviceKey);
            request.Headers.Add("x-agent-version", agentVersion);
            request.Content = JsonContent.Create(new { events = batch }, options: JsonOptions.Default);
            using var response = await client.SendAsync(request);
            response.EnsureSuccessStatusCode();
            pending.RemoveRange(0, batch.Length);
            if (File.Exists(spoolPath)) File.Delete(spoolPath);
        } catch (Exception error) {
            await File.AppendAllLinesAsync(spoolPath, batch.Select(item => JsonSerializer.Serialize(item, JsonOptions.Default)));
            pending.RemoveRange(0, batch.Length);
            Console.Error.WriteLine($"Sincronización pendiente: {error.Message}");
            await Heartbeat(error.Message);
        }
        nextSync = now.AddSeconds(Math.Max(30, config.SyncIntervalSeconds));
    }
    await Task.Delay(TimeSpan.FromSeconds(Math.Max(5, config.SampleIntervalSeconds)));
}

record AgentConfig(string ServerUrl, string DeviceId, string DeviceKey, int SampleIntervalSeconds = 15, int SyncIntervalSeconds = 60, bool CollectWindowTitles = true);
record ActivityEvent(string Id, string AppName, string WindowTitle, DateTimeOffset StartedAt, DateTimeOffset EndedAt, int DurationSeconds, int IdleSeconds);
record ForegroundSample(string AppName, string WindowTitle);

static class JsonOptions { public static readonly JsonSerializerOptions Default = new(JsonSerializerDefaults.Web); }

static class ForegroundReader {
    [StructLayout(LayoutKind.Sequential)] struct LASTINPUTINFO { public uint cbSize; public uint dwTime; }
    [DllImport("user32.dll")] static extern IntPtr GetForegroundWindow();
    [DllImport("user32.dll", SetLastError = true)] static extern uint GetWindowThreadProcessId(IntPtr window, out uint processId);
    [DllImport("user32.dll", CharSet = CharSet.Unicode)] static extern int GetWindowText(IntPtr window, char[] text, int count);
    [DllImport("user32.dll")] static extern bool GetLastInputInfo(ref LASTINPUTINFO info);

    public static ForegroundSample? Read() {
        var window = GetForegroundWindow();
        if (window == IntPtr.Zero) return null;
        GetWindowThreadProcessId(window, out var processId);
        try {
            using var process = Process.GetProcessById((int)processId);
            var buffer = new char[512];
            var length = GetWindowText(window, buffer, buffer.Length);
            return new ForegroundSample(process.ProcessName, length > 0 ? new string(buffer, 0, length) : "");
        } catch { return null; }
    }

    public static int IdleSeconds() {
        var info = new LASTINPUTINFO { cbSize = (uint)Marshal.SizeOf<LASTINPUTINFO>() };
        return GetLastInputInfo(ref info) ? Math.Max(0, (int)((Environment.TickCount64 - info.dwTime) / 1000)) : 0;
    }
}
