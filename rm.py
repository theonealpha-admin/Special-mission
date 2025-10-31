import psutil
import time
from datetime import datetime

def monitor_process_usage(pid_dict):
    while True:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{timestamp}]")
        
        for name, pid in pid_dict.items():
            try:
                proc = psutil.Process(pid)
                cpu = proc.cpu_percent(interval=0.5)
                mem_percent = proc.memory_percent()
                io = proc.io_counters()
                read_mb = io.read_bytes / (1024 * 1024)
                write_mb = io.write_bytes / (1024 * 1024)
                
                cpu_bar = "█" * int(cpu / 10) + "░" * (10 - int(cpu / 10))
                mem_bar = "█" * int(mem_percent / 10) + "░" * (10 - int(mem_percent / 10))
                
                print(f"{name:<15} PID:{pid:<6}")
                print(f"  CPU:{cpu:5.1f}% [{cpu_bar}] RAM:{mem_percent:5.2f}% [{mem_bar}]")
                print(f"  I/O: R:{read_mb:7.1f}MB W:{write_mb:7.1f}MB")
            except psutil.NoSuchProcess:
                print(f"{name:<15} PID:{pid:<6} ❌ DEAD")
        
        time.sleep(10)