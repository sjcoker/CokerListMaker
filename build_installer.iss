[Setup]
AppName=Coker's List Maker
AppVersion=10.5
AppPublisher=Steven James Coker
AppPublisherURL=https://github.com/sjcoker
DefaultDirName={autopf}\Coker List Maker
DefaultGroupName=Coker List Maker
AllowNoIcons=yes
OutputDir=.\Output
OutputBaseFilename=CokersListMaker_v10.5_Setup
SetupIconFile=assets\icon.ico
Compression=lzma
SolidCompression=yes
ArchitecturesInstallIn64BitMode=x64

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; GRAB THE ENTIRE .DIST FOLDER (This fixes the silent crash)
Source: "build_v10.5\CokerListMaker_v10.5.dist\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

; Include the assets folder so the About window can find the icon
Source: "assets\*"; DestDir: "{app}\assets"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
; USE {autoprograms} SO IT SHOWS UP IN WINDOWS 11 ALL APPS LIST
Name: "{autoprograms}\Coker's List Maker"; Filename: "{app}\CokerListMaker_v10.5.exe"; IconFilename: "{app}\assets\icon.ico"
Name: "{autodesktop}\Coker's List Maker"; Filename: "{app}\CokerListMaker_v10.5.exe"; Tasks: desktopicon; IconFilename: "{app}\assets\icon.ico"

[Run]
Filename: "{app}\CokerListMaker_v10.5.exe"; Description: "{cm:LaunchProgram,Coker's List Maker}"; Flags: nowait postinstall skipifsilent