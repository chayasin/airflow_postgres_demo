# PowerShell script to continuously pull latest code every 10 seconds
while ($true) {
    Write-Host "Pulling latest code from git..." -ForegroundColor Cyan
    git fetch
    git pull
    Start-Sleep -Seconds 10
}
