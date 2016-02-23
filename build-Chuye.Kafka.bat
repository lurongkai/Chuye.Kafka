@setlocal 
@set local=%~dp0
@pushd %WINDIR%\Microsoft.NET\Framework\v4.0.30319\
@goto build

:build
msbuild "%local%src\Chuye.Kafka.sln" /t:Rebuild /P:Configuration=Release
@goto copy

:copy
robocopy "%local%src\Chuye.Kafka\bin\Release" "%local%release\Chuye.Kafka" /mir
@goto end

:end
@pushd %local%
@pause