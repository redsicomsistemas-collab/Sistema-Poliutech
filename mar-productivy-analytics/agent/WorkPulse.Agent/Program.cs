using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows.Automation;

if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("MAR Productivy Analytics requiere Windows 10 u 11.");

var configPath = Path.Combine(AppContext.BaseDirectory, "agent.json");
if (!File.Exists(configPath)) {
    return 2;
}

var config = JsonSerializer.Deserialize<AgentConfig>(await File.ReadAllTextAsync(configPath), JsonOptions.Default)
             ?? throw new InvalidOperationException("La configuración no es válida.");
const string agentVersion = "2.1.1";
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

while (true) {
    var now = DateTimeOffset.UtcNow;
    if (now >= nextHeartbeat) { await Heartbeat(); nextHeartbeat=now.AddMinutes(1); }
    var current = ForegroundReader.Read();
    if (previous is not null && current is not null) {
        var elapsed = Math.Clamp((int)(now - previousAt).TotalSeconds, 1, 60);
        var idle = Math.Min(elapsed, ForegroundReader.IdleSeconds());
        pending.Add(new ActivityEvent(Guid.NewGuid().ToString("N"), previous.AppName, config.CollectWindowTitles ? previous.WindowTitle : "", previousAt, now, elapsed, idle, previous.Domain));
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
            await Heartbeat(error.Message);
        }
        nextSync = now.AddSeconds(Math.Max(30, config.SyncIntervalSeconds));
    }
    await Task.Delay(TimeSpan.FromSeconds(Math.Max(5, config.SampleIntervalSeconds)));
}

record AgentConfig(string ServerUrl, string DeviceId, string DeviceKey, int SampleIntervalSeconds = 15, int SyncIntervalSeconds = 60, bool CollectWindowTitles = true);
record ActivityEvent(string Id, string AppName, string WindowTitle, DateTimeOffset StartedAt, DateTimeOffset EndedAt, int DurationSeconds, int IdleSeconds, string? Domain);
record ForegroundSample(string AppName, string WindowTitle, string? Domain);

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
            var appName = process.ProcessName;
            return new ForegroundSample(appName, length > 0 ? new string(buffer, 0, length) : "", BrowserDomainReader.Read(window, appName));
        } catch { return null; }
    }

    public static int IdleSeconds() {
        var info = new LASTINPUTINFO { cbSize = (uint)Marshal.SizeOf<LASTINPUTINFO>() };
        return GetLastInputInfo(ref info) ? Math.Max(0, (int)((Environment.TickCount64 - info.dwTime) / 1000)) : 0;
    }
}

static class BrowserDomainReader {
    static readonly HashSet<string> Browsers = new(StringComparer.OrdinalIgnoreCase) { "chrome", "msedge", "firefox", "brave", "opera", "vivaldi" };

    public static string? Read(IntPtr window, string processName) {
        if (!Browsers.Contains(processName)) return null;
        try {
            var root = AutomationElement.FromHandle(window);
            var edits = root.FindAll(TreeScope.Descendants, new PropertyCondition(AutomationElement.ControlTypeProperty, ControlType.Edit));
            foreach (AutomationElement element in edits) {
                if (!element.TryGetCurrentPattern(ValuePattern.Pattern, out var pattern)) continue;
                var domain = ToDomain(((ValuePattern)pattern).Current.Value);
                if (domain is not null) return domain;
            }
        } catch { }
        return null;
    }

    static string? ToDomain(string? value) {
        value = value?.Trim();
        if (string.IsNullOrWhiteSpace(value) || value.Contains(' ')) return null;
        var candidate = value.Contains("://", StringComparison.Ordinal) ? value : $"https://{value}";
        if (!Uri.TryCreate(candidate, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https") || !uri.Host.Contains('.')) return null;
        var host = uri.IdnHost.ToLowerInvariant();
        return host.StartsWith("www.", StringComparison.Ordinal) ? host[4..] : host;
    }
}
