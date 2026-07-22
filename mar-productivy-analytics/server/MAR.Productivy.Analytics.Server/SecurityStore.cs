using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text.Json;

record LoginInput(string Username, string Password);
record UserInput(string Username, string Password, string Role);
record AdminUser(string Username, string PasswordHash, string Role, bool Active, DateTimeOffset CreatedAt);

sealed class SecurityStore {
    readonly object gate=new(); readonly string root, usersPath, auditPath; readonly JsonSerializerOptions json=new(JsonSerializerDefaults.Web){WriteIndented=true};
    readonly ConcurrentDictionary<string,(string user,string role,DateTimeOffset expires)> sessions=new(); List<AdminUser> users;
    public SecurityStore(){root=Environment.GetEnvironmentVariable("MAR_DATA_PATH")??Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),"MAR Productivy Analytics","Server");Directory.CreateDirectory(root);usersPath=Path.Combine(root,"users.json");auditPath=Path.Combine(root,"audit.jsonl");users=Load();if(users.Count==0){var password=Environment.GetEnvironmentVariable("MAR_ADMIN_PASSWORD");if(string.IsNullOrWhiteSpace(password))password=Convert.ToHexString(RandomNumberGenerator.GetBytes(10));users.Add(new("admin",Hash(password),"administrator",true,DateTimeOffset.UtcNow));Save();File.WriteAllText(Path.Combine(root,"CLAVE-INICIAL-ADMIN.txt"),$"Usuario: admin{Environment.NewLine}Contraseña: {password}{Environment.NewLine}Cambia esta contraseña después del primer acceso.");}}
    List<AdminUser> Load(){try{return File.Exists(usersPath)?JsonSerializer.Deserialize<List<AdminUser>>(File.ReadAllText(usersPath),json)??[]:[];}catch{return[];}}
    void Save()=>File.WriteAllText(usersPath,JsonSerializer.Serialize(users,json));
    static string Hash(string value){var salt="MAR-Productivy-Analytics-v3";return Convert.ToHexString(Rfc2898DeriveBytes.Pbkdf2(value,System.Text.Encoding.UTF8.GetBytes(salt),120000,HashAlgorithmName.SHA256,32));}
    public string? Login(string username,string password){lock(gate){var user=users.FirstOrDefault(x=>x.Active&&x.Username.Equals(username,StringComparison.OrdinalIgnoreCase)&&CryptographicOperations.FixedTimeEquals(Convert.FromHexString(x.PasswordHash),Convert.FromHexString(Hash(password))));if(user is null)return null;var token=Convert.ToHexString(RandomNumberGenerator.GetBytes(32));sessions[token]=(user.Username,user.Role,DateTimeOffset.UtcNow.AddHours(12));Audit(user.Username,"login","Inicio de sesión");return token;}}
    public (string user,string role)? Validate(string? token){if(string.IsNullOrWhiteSpace(token)||!sessions.TryGetValue(token,out var s)||s.expires<DateTimeOffset.UtcNow)return null;return(s.user,s.role);}
    public object ListUsers(){lock(gate)return users.Select(x=>new{x.Username,x.Role,x.Active,x.CreatedAt});}
    public object AddUser(UserInput input,string actor){lock(gate){if(users.Any(x=>x.Username.Equals(input.Username,StringComparison.OrdinalIgnoreCase)))throw new InvalidOperationException("El usuario ya existe.");var role=input.Role is "administrator" or "analyst" or "viewer"?input.Role:"viewer";users.Add(new(input.Username.Trim(),Hash(input.Password),role,true,DateTimeOffset.UtcNow));Save();Audit(actor,"user.create",input.Username);return new{input.Username,role};}}
    public void Logout(string? token){if(!string.IsNullOrWhiteSpace(token))sessions.TryRemove(token,out _);}
    public void Audit(string user,string action,string detail){var line=JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,user,action,detail});File.AppendAllText(auditPath,line+Environment.NewLine);}
    public object AuditLog(){try{var entries=new List<JsonElement>();if(File.Exists(auditPath))foreach(var line in File.ReadLines(auditPath).TakeLast(500)){using var document=JsonDocument.Parse(line);entries.Add(document.RootElement.Clone());}entries.Reverse();return new{entries};}catch{return new{entries=new List<JsonElement>()};}}
}
