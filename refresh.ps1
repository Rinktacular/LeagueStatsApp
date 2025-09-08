# refresh.ps1
param(
    [string[]]$Views = @(),     # optional list of views
    [switch]$NoAnalyze          # skip analyze if set
)

# Ensure token is available
if (-not $env:ADMIN_TOKEN) {
    Write-Error "Please set ADMIN_TOKEN in this PowerShell session or in your .env file."
    exit 1
}

$headers = @{ Authorization = "Bearer $env:ADMIN_TOKEN" }

# Build JSON body
$body = @{
    views        = if ($Views.Count -gt 0) { $Views } else { $null }
    analyze_after = (-not $NoAnalyze.IsPresent)
} | ConvertTo-Json -Compress

# Call the API
$response = Invoke-RestMethod -Method Post `
    -Uri "http://localhost:8000/admin/refresh" `
    -Headers $headers `
    -ContentType "application/json" `
    -Body $body

$response | ConvertTo-Json -Depth 5
