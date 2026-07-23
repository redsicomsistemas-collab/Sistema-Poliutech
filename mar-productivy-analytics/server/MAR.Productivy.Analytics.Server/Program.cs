using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.IO.Compression;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory, WebRootPath = "wwwroot" });
var renderPort=Environment.GetEnvironmentVariable("PORT");
builder.WebHost.UseUrls(Environment.GetEnvironmentVariable("MAR_SERVER_URLS") ?? (string.IsNullOrWhiteSpace(renderPort)?"http://0.0.0.0:5080":$"http://0.0.0.0:{renderPort}"));
builder.Services.AddSingleton<LocalStore>();
builder.Services.AddSingleton<SecurityStore>();
var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();
app.Use(async (context, next) => {
    var path=context.Request.Path.Value??"";
    var isEmployeeRegistration=context.Request.Method=="POST"&&path=="/api/devices";
    if(!path.StartsWith("/api/")||path is "/api/login" or "/api/ingest" or "/api/heartbeat"||isEmployeeRegistration){await next();return;}
    var security=context.RequestServices.GetRequiredService<SecurityStore>();var session=security.Validate(context.Request.Cookies["mar_session"]);
    if(session is null){context.Response.StatusCode=401;await context.Response.WriteAsJsonAsync(new{error="Inicia sesión"});return;}
    context.Items["user"]=session.Value.user;context.Items["role"]=session.Value.role;await next();
});

app.MapPost("/api/login", (LoginInput input, HttpResponse response, SecurityStore security) => {var token=security.Login(input.Username,input.Password);if(token is null)return Results.Unauthorized();response.Cookies.Append("mar_session",token,new(){HttpOnly=true,SameSite=SameSiteMode.Strict,Expires=DateTimeOffset.UtcNow.AddHours(12)});return Results.Ok(new{user=input.Username});});
app.MapPost("/api/logout", (HttpRequest request,HttpResponse response,SecurityStore security)=>{security.Logout(request.Cookies["mar_session"]);response.Cookies.Delete("mar_session");return Results.NoContent();});
app.MapGet("/api/users", (SecurityStore security)=>Results.Ok(new{users=security.ListUsers()}));
app.MapPost("/api/users", (UserInput input,HttpContext context,SecurityStore security)=>Results.Ok(security.AddUser(input,context.Items["user"]?.ToString()??"system")));
app.MapPut("/api/users/{username}/password", (string username,PasswordResetInput input,HttpContext context,SecurityStore security) => {
    if(context.Items["role"]?.ToString()!="administrator")return Results.Forbid();
    if(string.IsNullOrWhiteSpace(input.Password)||input.Password.Length<8)return Results.BadRequest(new{error="La contraseña debe tener al menos 8 caracteres."});
    return security.ResetPassword(username,input.Password,context.Items["user"]?.ToString()??"system")?Results.NoContent():Results.NotFound();
});
app.MapGet("/api/audit", (SecurityStore security)=>Results.Ok(security.AuditLog()));
app.MapGet("/api/backup", (LocalStore store)=>Results.File(store.Backup(),"application/zip",$"MAR-respaldo-{DateTime.Now:yyyyMMdd-HHmm}.zip"));
app.MapPost("/api/restore", async (HttpRequest request,LocalStore store)=>{if(!request.HasFormContentType)return Results.BadRequest(new{error="Selecciona un respaldo ZIP."});var form=await request.ReadFormAsync();var file=form.Files.FirstOrDefault();if(file is null)return Results.BadRequest(new{error="Falta el archivo."});await using var stream=file.OpenReadStream();store.Restore(stream);return Results.Ok(new{restored=true});});

app.MapGet("/api/server-info", (HttpRequest request) => {
    var publicUrl=Environment.GetEnvironmentVariable("MAR_PUBLIC_URL");
    if(!string.IsNullOrWhiteSpace(publicUrl)) return Results.Ok(new { serverUrl=publicUrl.TrimEnd('/') });
    var address = NetworkInterface.GetAllNetworkInterfaces()
        .Where(adapter => adapter.OperationalStatus == OperationalStatus.Up
            && adapter.NetworkInterfaceType is not NetworkInterfaceType.Loopback and not NetworkInterfaceType.Tunnel
            && !IsVirtualOrVpn(adapter))
        .SelectMany(adapter => adapter.GetIPProperties().GatewayAddresses
            .Where(gateway => gateway.Address.AddressFamily == AddressFamily.InterNetwork && !gateway.Address.Equals(IPAddress.Any))
            .SelectMany(_ => adapter.GetIPProperties().UnicastAddresses
                .Where(item => item.Address.AddressFamily == AddressFamily.InterNetwork)
                .Select(item => item.Address)))
        .FirstOrDefault(IsPrivateLanAddress);
    return Results.Ok(new { serverUrl = address is null ? $"{request.Scheme}://{request.Host}" : $"http://{address}:5080" });
});
app.MapGet("/api/dashboard", (LocalStore store) => Results.Ok(store.Dashboard()));
app.MapGet("/api/devices", (LocalStore store) => Results.Ok(store.Devices()));
app.MapGet("/api/activity", (LocalStore store) => Results.Ok(store.Activity()));
app.MapGet("/api/applications", (LocalStore store) => Results.Ok(store.Applications()));
app.MapGet("/api/report", (DateTimeOffset? from, DateTimeOffset? to, string? deviceId, LocalStore store) => Results.Ok(store.Report(from, to, deviceId)));
app.MapGet("/api/report.csv", (DateTimeOffset? from, DateTimeOffset? to, string? deviceId, LocalStore store) => Results.File(Encoding.UTF8.GetPreamble().Concat(Encoding.UTF8.GetBytes(store.ReportCsv(from, to, deviceId))).ToArray(), "text/csv; charset=utf-8", $"MAR-reporte-{DateTime.Now:yyyyMMdd}.csv"));
app.MapGet("/api/settings", (LocalStore store) => Results.Ok(store.Settings()));
app.MapPut("/api/settings", async (SystemSettings input, LocalStore store) => Results.Ok(store.SaveSettings(input)));
app.MapPut("/api/applications/{name}/category", async (string name, AppCategoryInput input, LocalStore store) => Results.Ok(store.SetCategory(name, input.Category)));
app.MapPost("/api/devices", async (HttpRequest request, LocalStore store) => {
    try {
        var input = await request.ReadFromJsonAsync<NewDevice>();
        if (input is null || string.IsNullOrWhiteSpace(input.EmployeeName) || string.IsNullOrWhiteSpace(input.ComputerName))
            return Results.BadRequest(new { error = "Nombre y computadora son obligatorios" });
        return Results.Json(store.AddDevice(input), statusCode: 201);
    } catch (JsonException) {
        return Results.BadRequest(new { error = "Los datos enviados por el instalador no tienen un formato válido." });
    } catch (InvalidOperationException error) {
        return Results.Conflict(new { error = error.Message });
    } catch (Exception error) {
        return Results.Problem(title: "No se pudo registrar el dispositivo", detail: error.Message, statusCode: 500);
    }
});
app.MapDelete("/api/devices/{id}", (string id, LocalStore store) => store.DeleteDevice(id) ? Results.NoContent() : Results.NotFound());
app.MapPost("/api/ingest", async (HttpRequest request, LocalStore store) => {
    var id = request.Headers["x-device-id"].ToString();
    var key = request.Headers["x-device-key"].ToString();
    if (!store.Authorize(id, key)) return Results.Unauthorized();
    var batch = await request.ReadFromJsonAsync<EventBatch>();
    var accepted = store.AddEvents(id, batch?.Events ?? []);
    store.UpdateConnection(id, request.Headers["x-agent-version"].ToString(), null);
    return Results.Ok(new { accepted, serverTime = DateTimeOffset.UtcNow });
});
app.MapPost("/api/heartbeat", (HttpRequest request, LocalStore store) => {
    var id=request.Headers["x-device-id"].ToString(); var key=request.Headers["x-device-key"].ToString();
    if(!store.Authorize(id,key)) return Results.Unauthorized();
    store.UpdateConnection(id,request.Headers["x-agent-version"].ToString(),request.Headers["x-agent-error"].ToString());
    return Results.Ok(new { serverTime=DateTimeOffset.UtcNow });
});
app.MapFallbackToFile("index.html");
app.Run();

static bool IsVirtualOrVpn(NetworkInterface adapter) {
    var label = $"{adapter.Name} {adapter.Description}".ToLowerInvariant();
    string[] excluded = ["vpn", "tailscale", "wireguard", "wsl", "hyper-v", "vethernet", "virtual", "loopback", "tunnel"];
    return excluded.Any(label.Contains);
}

static bool IsPrivateLanAddress(IPAddress address) {
    var bytes = address.GetAddressBytes();
    return bytes[0] == 10
        || (bytes[0] == 172 && bytes[1] is >= 16 and <= 31)
        || (bytes[0] == 192 && bytes[1] == 168);
}

record NewDevice(string EmployeeName, string ComputerName, string? Team, string? DeviceId = null, string? DeviceKey = null, bool ConsentAccepted = false, string? ConsentVersion = null);
record EventBatch(List<ActivityEvent> Events);
record ActivityEvent(string Id, string AppName, string? WindowTitle, DateTimeOffset StartedAt, DateTimeOffset EndedAt, int DurationSeconds, int IdleSeconds);
record Device(string Id, string EmployeeName, string ComputerName, string Team, string SecretHash, DateTimeOffset CreatedAt, DateTimeOffset? LastSeenAt, string? AgentVersion = null, string? LastError = null, DateTimeOffset? ConsentAcceptedAt = null, string? ConsentVersion = null);

sealed class LocalStore {
    readonly object gate = new();
    readonly string dataDirectory;
    readonly string devicesPath;
    readonly string eventsPath;
    readonly string settingsPath;
    readonly JsonSerializerOptions json = new(JsonSerializerDefaults.Web) { WriteIndented = true };
    List<Device> devices;
    List<ActivityEventRecord> events;
    SystemSettings settings;

    public LocalStore() {
        dataDirectory = Environment.GetEnvironmentVariable("MAR_DATA_PATH") ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "MAR Productivy Analytics", "Server");
        Directory.CreateDirectory(dataDirectory);
        devicesPath = Path.Combine(dataDirectory, "devices.json");
        eventsPath = Path.Combine(dataDirectory, "activity.json");
        settingsPath = Path.Combine(dataDirectory, "settings.json");
        devices = Load<Device>(devicesPath);
        events = Load<ActivityEventRecord>(eventsPath);
        settings = LoadObject(settingsPath, new SystemSettings());
    }

    List<T> Load<T>(string path) {
        try { return File.Exists(path) ? JsonSerializer.Deserialize<List<T>>(File.ReadAllText(path), json) ?? [] : []; }
        catch { return []; }
    }
    T LoadObject<T>(string path, T fallback) {
        try { return File.Exists(path) ? JsonSerializer.Deserialize<T>(File.ReadAllText(path), json) ?? fallback : fallback; }
        catch { return fallback; }
    }
    void Save() {
        File.WriteAllText(devicesPath, JsonSerializer.Serialize(devices, json));
        File.WriteAllText(eventsPath, JsonSerializer.Serialize(events.TakeLast(250000), json));
    }
    static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    (string category, double score) Classify(string app) {
        var value = app.ToLowerInvariant();
        var custom = settings.ApplicationCategories?.FirstOrDefault(item => item.Key.Equals(app, StringComparison.OrdinalIgnoreCase)).Value;
        if (!string.IsNullOrWhiteSpace(custom)) return custom.ToLowerInvariant() switch { "productive" or "productivo" => ("productive", 1), "distracting" or "improductivo" => ("distracting", -1), _ => ("neutral", 0) };
        string[] productive = ["code", "visual studio", "excel", "word", "powerpoint", "teams", "slack", "outlook"];
        string[] distracting = ["netflix", "steam", "tiktok", "facebook", "instagram"];
        if (productive.Any(value.Contains)) return ("productive", 1);
        if (distracting.Any(value.Contains)) return ("distracting", -1);
        return ("neutral", 0);
    }

    public object AddDevice(NewDevice input) { lock (gate) {
        var key = string.IsNullOrWhiteSpace(input.DeviceKey) ? Convert.ToHexString(RandomNumberGenerator.GetBytes(32)).ToLowerInvariant() : input.DeviceKey;
        var id = string.IsNullOrWhiteSpace(input.DeviceId) ? Guid.NewGuid().ToString() : input.DeviceId;
        if (devices.Any(item => item.Id == id)) throw new InvalidOperationException("El dispositivo ya está registrado.");
        var device = new Device(id, input.EmployeeName.Trim(), input.ComputerName.Trim(), string.IsNullOrWhiteSpace(input.Team) ? "General" : input.Team.Trim(), Hash(key), DateTimeOffset.UtcNow, null, null, null, input.ConsentAccepted ? DateTimeOffset.UtcNow : null, input.ConsentVersion);
        devices.Add(device); Save();
        return new { device = new { device.Id, device.EmployeeName, device.ComputerName, device.Team }, deviceKey = key };
    }}
    public bool Authorize(string id, string key) { lock (gate) { return devices.Any(d => d.Id == id && CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(d.SecretHash), Encoding.ASCII.GetBytes(Hash(key)))); }}
    public int AddEvents(string deviceId, List<ActivityEvent> incoming) { lock (gate) {
        var known = events.Select(e => e.Id).ToHashSet();
        var valid = incoming.Take(500).Where(e => !known.Contains(e.Id) && !string.IsNullOrWhiteSpace(e.AppName) && e.DurationSeconds is > 0 and <= 86400).ToList();
        foreach (var item in valid) { var type = Classify(item.AppName); events.Add(new(item.Id, deviceId, item.AppName[..Math.Min(item.AppName.Length, 160)], (item.WindowTitle ?? "")[..Math.Min((item.WindowTitle ?? "").Length, 300)], item.StartedAt, item.EndedAt, item.DurationSeconds, Math.Max(0, item.IdleSeconds), type.category, type.score)); }
        var index = devices.FindIndex(d => d.Id == deviceId); if (index >= 0) devices[index] = devices[index] with { LastSeenAt = DateTimeOffset.UtcNow };
        Save(); return valid.Count;
    }}
    public void UpdateConnection(string deviceId, string? version, string? error) { lock(gate) {
        var index=devices.FindIndex(item=>item.Id==deviceId); if(index<0)return;
        devices[index]=devices[index] with { LastSeenAt=DateTimeOffset.UtcNow, AgentVersion=string.IsNullOrWhiteSpace(version)?devices[index].AgentVersion:version, LastError=string.IsNullOrWhiteSpace(error)?null:error[..Math.Min(error.Length,300)] };
        Save();
    }}
    public object Devices() { lock (gate) { return new { devices = devices.Select(d => new { d.Id, d.EmployeeName, d.ComputerName, d.Team, d.CreatedAt, d.LastSeenAt, d.AgentVersion, d.LastError, d.ConsentAcceptedAt, d.ConsentVersion, status = d.LastSeenAt > DateTimeOffset.UtcNow.AddMinutes(-5) ? "online" : d.LastSeenAt is null ? "never" : "offline" }) }; }}
    public bool DeleteDevice(string id) { lock (gate) {
        var removed = devices.RemoveAll(device => device.Id == id) > 0;
        if (!removed) return false;
        events.RemoveAll(item => item.DeviceId == id);
        Save();
        return true;
    }}
    public object Activity() { lock (gate) {
        var names = devices.ToDictionary(device => device.Id, device => new { device.EmployeeName, device.ComputerName });
        var items = events.OrderByDescending(item => item.StartedAt).Take(500).Select(item => new {
            item.Id, item.DeviceId,
            employeeName = names.TryGetValue(item.DeviceId, out var device) ? device.EmployeeName : "Desconocido",
            computerName = names.TryGetValue(item.DeviceId, out device) ? device.ComputerName : "Desconocido",
            item.AppName, item.WindowTitle, item.StartedAt, item.EndedAt, item.DurationSeconds, item.IdleSeconds, item.Category
        });
        return new { activity = items };
    }}
    public object Applications() { lock (gate) {
        var since = DateTimeOffset.UtcNow.AddDays(-7);
        var applications = events.Where(item => item.StartedAt >= since).GroupBy(item => new { item.AppName, item.Category })
            .Select(group => new { name = group.Key.AppName, category = group.Key.Category, seconds = group.Sum(item => Math.Max(0, item.DurationSeconds - item.IdleSeconds)), uses = group.Count(), lastUsedAt = group.Max(item => item.EndedAt) })
            .OrderByDescending(item => item.seconds).ToList();
        return new { applications };
    }}
    public object Settings() { lock (gate) { return settings; }}
    public byte[] Backup() { lock(gate) { using var memory=new MemoryStream();using(var archive=new ZipArchive(memory,ZipArchiveMode.Create,true))foreach(var path in new[]{devicesPath,eventsPath,settingsPath})if(File.Exists(path)){var entry=archive.CreateEntry(Path.GetFileName(path),CompressionLevel.Optimal);using var target=entry.Open();using var source=File.OpenRead(path);source.CopyTo(target);}return memory.ToArray(); }}
    public void Restore(Stream source) { lock(gate) {
        using var archive=new ZipArchive(source,ZipArchiveMode.Read,true);var allowed=new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase){{"devices.json",devicesPath},{"activity.json",eventsPath},{"settings.json",settingsPath}};
        foreach(var entry in archive.Entries){if(!allowed.TryGetValue(entry.Name,out var target))continue;using var input=entry.Open();using var output=File.Create(target+".restore");input.CopyTo(output);output.Close();File.Move(target+".restore",target,true);}
        devices=Load<Device>(devicesPath);events=Load<ActivityEventRecord>(eventsPath);settings=LoadObject(settingsPath,new SystemSettings());
    }}
    public object SaveSettings(SystemSettings input) { lock (gate) {
        settings = input with { ApplicationCategories = input.ApplicationCategories ?? settings.ApplicationCategories ?? new() };
        File.WriteAllText(settingsPath, JsonSerializer.Serialize(settings, json));
        return settings;
    }}
    public object SetCategory(string name, string category) { lock (gate) {
        var categories = settings.ApplicationCategories is null ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) : new Dictionary<string, string>(settings.ApplicationCategories, StringComparer.OrdinalIgnoreCase);
        categories[name] = category;
        settings = settings with { ApplicationCategories = categories };
        File.WriteAllText(settingsPath, JsonSerializer.Serialize(settings, json));
        for (var index = 0; index < events.Count; index++) if (events[index].AppName.Equals(name, StringComparison.OrdinalIgnoreCase)) { var value = Classify(name); events[index] = events[index] with { Category = value.category, ProductivityScore = value.score }; }
        Save();
        return new { name, category };
    }}
    public object Report(DateTimeOffset? from, DateTimeOffset? to, string? deviceId) { lock (gate) {
        var start = from ?? DateTimeOffset.UtcNow.AddDays(-7); var end = to ?? DateTimeOffset.UtcNow;
        var selected = events.Where(item => item.StartedAt >= start && item.StartedAt <= end && (string.IsNullOrWhiteSpace(deviceId) || item.DeviceId == deviceId)).ToList();
        var active = selected.Sum(item => Math.Max(0, item.DurationSeconds - item.IdleSeconds));
        var idle = selected.Sum(item => Math.Min(item.DurationSeconds, item.IdleSeconds));
        var productive = selected.Where(item => item.ProductivityScore > 0).Sum(item => Math.Max(0, item.DurationSeconds - item.IdleSeconds));
        var distracting = selected.Where(item => item.ProductivityScore < 0).Sum(item => Math.Max(0, item.DurationSeconds - item.IdleSeconds));
        var byDay = selected.GroupBy(item => item.StartedAt.ToLocalTime().Date).OrderBy(group => group.Key).Select(group => new { date = group.Key.ToString("yyyy-MM-dd"), activeSeconds = group.Sum(item => Math.Max(0, item.DurationSeconds-item.IdleSeconds)), productiveSeconds = group.Where(item => item.ProductivityScore > 0).Sum(item => Math.Max(0, item.DurationSeconds-item.IdleSeconds)), idleSeconds = group.Sum(item => Math.Min(item.DurationSeconds,item.IdleSeconds)) });
        var byPerson = devices.Where(device => string.IsNullOrWhiteSpace(deviceId) || device.Id == deviceId).Select(device => { var own=selected.Where(item=>item.DeviceId==device.Id).ToList(); return new { device.Id, device.EmployeeName, device.ComputerName, device.Team, activeSeconds=own.Sum(item=>Math.Max(0,item.DurationSeconds-item.IdleSeconds)), productiveSeconds=own.Where(item=>item.ProductivityScore>0).Sum(item=>Math.Max(0,item.DurationSeconds-item.IdleSeconds)), firstActivity=own.Count==0?(DateTimeOffset?)null:own.Min(item=>item.StartedAt), lastActivity=own.Count==0?(DateTimeOffset?)null:own.Max(item=>item.EndedAt) }; });
        var applications = selected.GroupBy(item => new { item.AppName, item.Category }).Select(group => new { name=group.Key.AppName, category=group.Key.Category, seconds=group.Sum(item=>Math.Max(0,item.DurationSeconds-item.IdleSeconds)), uses=group.Count() }).OrderByDescending(item=>item.seconds);
        return new { from=start, to=end, summary=new { activeSeconds=active, idleSeconds=idle, productiveSeconds=productive, distractingSeconds=distracting, productivity=active==0?0:productive*100.0/active }, byDay, byPerson, applications };
    }}
    public string ReportCsv(DateTimeOffset? from, DateTimeOffset? to, string? deviceId) { lock (gate) {
        var start=from??DateTimeOffset.UtcNow.AddDays(-7); var end=to??DateTimeOffset.UtcNow;
        static string Csv(string value) => $"\"{value.Replace("\"", "\"\"")}\"";
        var lines = new List<string> { "Empleado,Computadora,Departamento,Aplicación,Ventana,Inicio,Fin,Segundos activos,Segundos inactivos,Categoría" };
        var names=devices.ToDictionary(item=>item.Id);
        foreach(var item in events.Where(item=>item.StartedAt>=start&&item.StartedAt<=end&&(string.IsNullOrWhiteSpace(deviceId)||item.DeviceId==deviceId)).OrderBy(item=>item.StartedAt)) { names.TryGetValue(item.DeviceId,out var device); lines.Add(string.Join(",", Csv(device?.EmployeeName??"Desconocido"),Csv(device?.ComputerName??""),Csv(device?.Team??""),Csv(item.AppName),Csv(item.WindowTitle),Csv(item.StartedAt.ToLocalTime().ToString("s")),Csv(item.EndedAt.ToLocalTime().ToString("s")),Math.Max(0,item.DurationSeconds-item.IdleSeconds),item.IdleSeconds,Csv(item.Category))); }
        return string.Join("\r\n",lines);
    }}
    public object Dashboard() { lock (gate) {
        var since = DateTimeOffset.UtcNow.AddDays(-7); var recent = events.Where(e => e.StartedAt >= since).ToList();
        var active = recent.Sum(e => Math.Max(0, e.DurationSeconds - e.IdleSeconds));
        var focus = recent.Where(e => e.ProductivityScore > 0).Sum(e => Math.Max(0, e.DurationSeconds - e.IdleSeconds));
        var people = devices.Select(d => { var own = recent.Where(e => e.DeviceId == d.Id).ToList(); var ownActive = own.Sum(e => Math.Max(0, e.DurationSeconds - e.IdleSeconds)); var ownFocus = own.Where(e => e.ProductivityScore > 0).Sum(e => Math.Max(0, e.DurationSeconds - e.IdleSeconds)); return new { id=d.Id, name=d.EmployeeName, team=d.Team, activeSeconds=ownActive, focusSeconds=ownFocus, lastSeenAt=d.LastSeenAt }; }).OrderByDescending(p => p.activeSeconds);
        var apps = recent.GroupBy(e => new { e.AppName, e.Category }).Select(g => new { name=g.Key.AppName, category=g.Key.Category, seconds=g.Sum(e => Math.Max(0, e.DurationSeconds-e.IdleSeconds)) }).OrderByDescending(a => a.seconds).Take(8);
        return new { live=true, summary=new { activeSeconds=active, focusSeconds=focus, productivity=active == 0 ? 0 : focus*100.0/active, activePeople=devices.Count(d => d.LastSeenAt > DateTimeOffset.UtcNow.AddMinutes(-5)), totalPeople=devices.Count }, people, apps };
    }}
}

record ActivityEventRecord(string Id, string DeviceId, string AppName, string WindowTitle, DateTimeOffset StartedAt, DateTimeOffset EndedAt, int DurationSeconds, int IdleSeconds, string Category, double ProductivityScore);
record AppCategoryInput(string Category);
record SystemSettings(string CompanyName = "MAR · POLIUTECH", string WorkdayStart = "09:00", string WorkdayEnd = "18:00", int IdleThresholdMinutes = 5, int RetentionDays = 365, bool CollectWindowTitles = true, Dictionary<string, string>? ApplicationCategories = null, string TimeZoneId = "Central Standard Time (Mexico)", string WorkDays = "1,2,3,4,5", string PrivacyNotice = "Este equipo registra aplicaciones, títulos de ventana, actividad e inactividad con fines de productividad. No registra teclas ni audio.", string ConsentVersion = "1.0");
