"""
Windows Service wrapper for query_history_logger.py

This allows the logger to run as a Windows service for 24/7 operation.

Installation:
    1. Install pywin32: pip install pywin32
    2. Install service: python query_history_service.py install
    3. Start service: python query_history_service.py start
    
Management:
    - Start: python query_history_service.py start
    - Stop: python query_history_service.py stop
    - Remove: python query_history_service.py remove
    - Check status: python query_history_service.py status
"""
import sys
import os
import time
import servicemanager
import win32event
import win32service
import win32serviceutil

# Import the logger functionality
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from query_history_logger import capture_and_log, CAPTURE_INTERVAL_SECONDS, LOG_FILE


class QueryHistoryLoggerService(win32serviceutil.ServiceFramework):
    """Windows service for query history logging."""
    
    _svc_name_ = "FabricQueryHistoryLogger"
    _svc_display_name_ = "Fabric Query History Logger"
    _svc_description_ = "Captures and logs Fabric lakehouse query history every 5 minutes"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True
    
    def SvcStop(self):
        """Handle stop request."""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        self.running = False
    
    def SvcDoRun(self):
        """Main service loop."""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, f"Started logging to {LOG_FILE}")
        )
        
        self.main()
    
    def main(self):
        """Service main loop - same as the standalone script."""
        while self.running:
            try:
                # Capture and log query history
                capture_and_log()
                
                # Wait for the configured interval or until stop signal
                result = win32event.WaitForSingleObject(
                    self.stop_event,
                    CAPTURE_INTERVAL_SECONDS * 1000  # Convert to milliseconds
                )
                
                # If stop event was signaled, exit
                if result == win32event.WAIT_OBJECT_0:
                    break
                    
            except Exception as e:
                # Log error and continue
                servicemanager.LogErrorMsg(f"Error in capture loop: {e}")
                # Wait a bit before retrying
                win32event.WaitForSingleObject(self.stop_event, 60000)  # 1 minute


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments - try to start as service
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(QueryHistoryLoggerService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Handle command line arguments (install, start, stop, etc.)
        win32serviceutil.HandleCommandLine(QueryHistoryLoggerService)
