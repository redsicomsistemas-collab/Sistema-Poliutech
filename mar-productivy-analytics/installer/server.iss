#define AppName "MAR Productivy Analytics Server"
#define AppVersion "2.0.0"
#define SourceDir "..\server\publish"

[Setup]
AppId={{8CA59C93-3C86-4A08-90FB-22E64A39F965}
AppName={#AppName}
AppVersion={#AppVersion}
DefaultDirName={autopf}\MAR Productivy Analytics Server
DefaultGroupName=MAR Productivy Analytics
OutputDir=output
OutputBaseFilename=Instalador Servidor - MAR Productivy Analytics
Compression=lzma2/max
SolidCompression=yes
PrivilegesRequired=admin
WizardStyle=modern
UninstallDisplayIcon={app}\MAR.Productivy.Analytics.Server.exe
SetupIconFile=assets\mar.ico
WizardImageFile=assets\login-roof-hero.png
WizardSmallImageFile=assets\logo-light-mark.png

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "assets\mar.ico"; DestDir: "{app}"; DestName: "MAR.ico"; Flags: ignoreversion

[Icons]
Name: "{group}\Abrir MAR Productivy Analytics"; Filename: "http://localhost:5080"
Name: "{commondesktop}\MAR Productivy Analytics"; Filename: "http://localhost:5080"

[Registry]
Root: HKLM; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "MAR Productivy Analytics Server"; ValueData: """{app}\MAR.Productivy.Analytics.Server.exe"""; Flags: uninsdeletevalue

[Run]
Filename: "{sys}\netsh.exe"; Parameters: "advfirewall firewall delete rule name=""MAR Productivy Analytics Server"""; Flags: runhidden waituntilterminated
Filename: "{sys}\netsh.exe"; Parameters: "advfirewall firewall add rule name=""MAR Productivy Analytics Server"" dir=in action=allow protocol=TCP localport=5080"; Flags: runhidden waituntilterminated
Filename: "{app}\MAR.Productivy.Analytics.Server.exe"; Flags: runhidden nowait
Filename: "{cmd}"; Parameters: "/C timeout /T 3 /NOBREAK >NUL"; Flags: runhidden waituntilterminated
Filename: "http://localhost:5080"; Description: "Abrir el panel local"; Flags: postinstall shellexec skipifsilent

[UninstallRun]
Filename: "{sys}\taskkill.exe"; Parameters: "/IM MAR.Productivy.Analytics.Server.exe /F"; Flags: runhidden
Filename: "{sys}\netsh.exe"; Parameters: "advfirewall firewall delete rule name=""MAR Productivy Analytics Server"""; Flags: runhidden

[Code]
procedure CurStepChanged(CurStep: TSetupStep);
var ResultCode: Integer;
begin
  if CurStep = ssInstall then
    Exec(ExpandConstant('{sys}\taskkill.exe'), '/IM MAR.Productivy.Analytics.Server.exe /F', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;
