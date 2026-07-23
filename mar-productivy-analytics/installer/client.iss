#define AppName "MAR Productivy Analytics Agent"
#define AppVersion "2.0.0"
#define AgentExe "..\agent\publish\win-x64\MAR.Productivy.Analytics.Agent.exe"

[Setup]
AppId={{2E270A2F-C6C9-4FBF-9271-F1598B717F11}
AppName={#AppName}
AppVersion={#AppVersion}
DefaultDirName={autopf}\MAR Productivy Analytics Agent
DefaultGroupName=MAR Productivy Analytics
OutputDir=output
OutputBaseFilename=Instalador Empleado - MAR Productivy Analytics
Compression=lzma2/max
SolidCompression=yes
PrivilegesRequired=admin
WizardStyle=modern
UninstallDisplayIcon={app}\MAR.Productivy.Analytics.Agent.exe
SetupIconFile=assets\mar.ico
WizardImageFile=assets\login-roof-hero.png
WizardSmallImageFile=assets\logo-light-mark.png

[Files]
Source: "{#AgentExe}"; DestDir: "{app}"; Flags: ignoreversion
Source: "assets\mar.ico"; DestDir: "{app}"; DestName: "MAR.ico"; Flags: ignoreversion

[Registry]
Root: HKLM; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "MAR Productivy Analytics Agent"; ValueData: """{app}\MAR.Productivy.Analytics.Agent.exe"""; Flags: uninsdeletevalue

[Run]
Filename: "{sys}\taskkill.exe"; Parameters: "/IM MAR.Productivy.Analytics.Agent.exe /F"; Flags: runhidden waituntilterminated
Filename: "{sys}\schtasks.exe"; Parameters: "/Delete /TN ""MAR Productivy Analytics Agent"" /F"; Flags: runhidden waituntilterminated
Filename: "{app}\MAR.Productivy.Analytics.Agent.exe"; Flags: runhidden nowait runasoriginaluser

[UninstallRun]
Filename: "{sys}\schtasks.exe"; Parameters: "/End /TN ""MAR Productivy Analytics Agent"""; Flags: runhidden
Filename: "{sys}\schtasks.exe"; Parameters: "/Delete /TN ""MAR Productivy Analytics Agent"" /F"; Flags: runhidden

[Code]
var
  DetailsPage: TInputQueryWizardPage;
  ConsentPage: TInputOptionWizardPage;
  DeviceId, DeviceKey, ConfigText: String;
  Registered: Boolean;

function JsonEscape(Value: String): String;
begin
  StringChangeEx(Value, '\', '\\', True);
  StringChangeEx(Value, '"', '\"', True);
  Result := Value;
end;

function NewGuid: String;
var
  TypeLib: Variant;
  RawGuid: String;
begin
  TypeLib := CreateOleObject('Scriptlet.TypeLib');
  RawGuid := TypeLib.Guid;
  Result := Trim(RawGuid);
  StringChangeEx(Result, '{', '', True);
  StringChangeEx(Result, '}', '', True);
end;

procedure InitializeWizard;
begin
  DetailsPage := CreateInputQueryPage(wpSelectDir, 'Conectar con el servidor', 'Datos de esta computadora', 'El instalador registrará este equipo automáticamente en tu panel local.');
  DetailsPage.Add('Dirección del servidor (ejemplo: http://192.168.1.20:5080):', False);
  DetailsPage.Add('Nombre del empleado:', False);
  DetailsPage.Add('Nombre de la computadora:', False);
  DetailsPage.Add('Equipo o departamento:', False);
  DetailsPage.Values[0] := 'http://192.168.1.20:5080';
  DetailsPage.Values[2] := GetComputerNameString;
  DetailsPage.Values[3] := 'General';
  ConsentPage := CreateInputOptionPage(DetailsPage.ID, 'Privacidad y consentimiento', 'Aviso para el empleado', 'MAR Productivy Analytics registra aplicaciones utilizadas, títulos de ventana, tiempo activo e inactividad. No registra teclas, contraseñas, audio ni cámara. La información se envía únicamente al servidor local de la organización.', False, True);
  ConsentPage.Add('He leído el aviso y acepto el monitoreo laboral informado.');
  ConsentPage.Selected[0] := False;
end;

function RegisterDevice: Boolean;
var
  Http: Variant;
  Url, Body: String;
begin
  Result := False;
  Url := RemoveBackslashUnlessRoot(DetailsPage.Values[0]) + '/api/devices';
  DeviceId := NewGuid;
  DeviceKey := GetSHA256OfString(NewGuid + NewGuid + GetDateTimeString('yyyymmddhhnnsszzz', '-', ':'));
  Body := '{"employeeName":"' + JsonEscape(DetailsPage.Values[1]) + '","computerName":"' + JsonEscape(DetailsPage.Values[2]) + '","team":"' + JsonEscape(DetailsPage.Values[3]) + '","deviceId":"' + DeviceId + '","deviceKey":"' + DeviceKey + '","consentAccepted":true,"consentVersion":"1.0"}';
  try
    { MSXML sends Unicode request strings as UTF-8 JSON correctly. }
    Http := CreateOleObject('MSXML2.ServerXMLHTTP.6.0');
    Http.open('POST', Url, False);
    Http.setRequestHeader('Content-Type', 'application/json; charset=utf-8');
    Http.setTimeouts(5000, 5000, 10000, 10000);
    Http.send(Body);
    if Http.status = 201 then begin
      ConfigText := '{' + #13#10 + '  "serverUrl": "' + JsonEscape(DetailsPage.Values[0]) + '",' + #13#10 + '  "deviceId": "' + DeviceId + '",' + #13#10 + '  "deviceKey": "' + DeviceKey + '",' + #13#10 + '  "sampleIntervalSeconds": 15,' + #13#10 + '  "syncIntervalSeconds": 60,' + #13#10 + '  "collectWindowTitles": true' + #13#10 + '}';
      Result := True;
    end else
      MsgBox('El servidor respondió con error ' + IntToStr(Http.status) + '.' + #13#10 + Http.responseText, mbError, MB_OK);
  except
    MsgBox('No se pudo completar el registro.' + #13#10 + 'Detalle técnico: ' + GetExceptionMessage, mbError, MB_OK);
  end;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;
  if CurPageID = DetailsPage.ID then begin
    if (Trim(DetailsPage.Values[0]) = '') or (Trim(DetailsPage.Values[1]) = '') or (Trim(DetailsPage.Values[2]) = '') then begin
      MsgBox('Completa servidor, empleado y computadora.', mbError, MB_OK);
      Result := False;
      exit;
    end;
  end;
  if CurPageID = ConsentPage.ID then begin
    if not ConsentPage.Selected[0] then begin
      MsgBox('Debes aceptar el aviso informado para instalar el agente.', mbError, MB_OK);
      Result := False;
      exit;
    end;
    Registered := RegisterDevice;
    Result := Registered;
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var ResultCode: Integer;
begin
  if CurStep = ssInstall then
    Exec(ExpandConstant('{sys}\taskkill.exe'), '/IM MAR.Productivy.Analytics.Agent.exe /F', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if (CurStep = ssPostInstall) and Registered then
    SaveStringToFile(ExpandConstant('{app}\agent.json'), ConfigText, False);
end;
