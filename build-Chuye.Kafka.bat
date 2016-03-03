@setlocal 
@set local=%~dp0

@pushd %WINDIR%\Microsoft.NET\Framework\v4.0.30319\
@goto build

:build
msbuild "%local%src\Chuye.Kafka.sln" /t:Rebuild /P:Configuration=Release
@goto pack

:pack
@pushd "%local%"
.nuget\NuGet.exe pack Chuye.Kafka.nuspec -Prop Configuration=Release -OutputDirectory release
@goto copy

:copy
robocopy "%local%src\Chuye.Kafka\bin\Release" "%local%release\Chuye.Kafka" /mir
@goto end



:end
@pushd %local%
@pause