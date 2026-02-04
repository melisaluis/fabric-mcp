# Query History Logger - 24/7 Setup Guide

This guide shows how to set up the query history logger for continuous 24/7 operation.

## Current Status
✓ Logger is currently running for testing (started via `python query_history_logger.py`)
✓ Captures query history every 5 minutes
✓ Logs to: `query_history_log.jsonl`

## For 24/7 Operation (When Ready)

### Option 1: Windows Service (Recommended)

**Advantages:**
- Runs even when you're logged out
- Automatically restarts if it crashes
- Starts on system boot
- Professional solution for production

**Setup Steps:**

1. **Install pywin32 package:**
   ```powershell
   pip install pywin32
   ```

2. **Install the service:**
   ```powershell
   python query_history_service.py install
   ```

3. **Start the service:**
   ```powershell
   python query_history_service.py start
   ```

4. **Verify it's running:**
   ```powershell
   python query_history_service.py status
   # Or check Windows Services (services.msc) - look for "Fabric Query History Logger"
   ```

**Management Commands:**
```powershell
# Stop the service
python query_history_service.py stop

# Remove the service
python query_history_service.py remove

# Restart the service
python query_history_service.py restart
```

### Option 2: Task Scheduler (Runs when logged in)

**Advantages:**
- Simpler setup
- Good for personal workstations
- Runs when you're logged in

**Setup:**
```powershell
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "C:\Users\v-melisaluis\OneDrive - Microsoft\Desktop\fabric-rti-mcp\query_history_logger.py" `
    -WorkingDirectory "C:\Users\v-melisaluis\OneDrive - Microsoft\Desktop\fabric-rti-mcp"

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName "FabricQueryHistoryLogger" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Captures Fabric lakehouse query history every 5 minutes"
```

**Management:**
```powershell
# Start the task
Start-ScheduledTask -TaskName "FabricQueryHistoryLogger"

# Stop the task
Stop-ScheduledTask -TaskName "FabricQueryHistoryLogger"

# Remove the task
Unregister-ScheduledTask -TaskName "FabricQueryHistoryLogger" -Confirm:$false
```

## Configuration Options

Edit `query_history_logger.py` to customize:

```python
# How often to capture (in seconds)
CAPTURE_INTERVAL_SECONDS = 300  # 5 minutes (change to 60 for every minute)

# Log file location
LOG_FILE = "query_history_log.jsonl"  # Change to absolute path if needed

# Number of queries to capture each time
top_n = 100  # In capture_and_log() function
```

## Monitoring

**Check if it's working:**
```powershell
# View the log file (most recent entries)
Get-Content query_history_log.jsonl -Tail 10

# Count how many entries
(Get-Content query_history_log.jsonl).Count

# View entries from last hour
python query_history_reader.py
```

**View log file size:**
```powershell
Get-Item query_history_log.jsonl | Select-Object Name, Length, LastWriteTime
```

## Troubleshooting

**Service won't start:**
- Check Windows Event Viewer → Windows Logs → Application
- Look for "Fabric Query History Logger" events
- Ensure SQL endpoint credentials are in environment variables

**High disk usage:**
- The log file grows over time
- Consider rotating logs or purging old entries
- Each capture adds ~5-10KB depending on query volume

**Authentication issues:**
- Service needs to run under your user account for Azure auth
- Install service with: `python query_history_service.py --username DOMAIN\username --password YOUR_PASSWORD install`

## Next Steps

When you're ready to switch from testing to 24/7:
1. Stop the current test process (Ctrl+C in the terminal)
2. Choose Windows Service (Option 1) or Task Scheduler (Option 2)
3. Follow the setup steps above
4. Verify it's running with `python query_history_reader.py`
