import psutil
import time
from datetime import datetime

def monitor_process_usage(pid_dict):
    while True:
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*70}")
        print(f"⏰ {timestamp}")
        print(f"{'='*70}")
        
        for name, pid in pid_dict.items():
            try:
                proc = psutil.Process(pid)
                cpu = proc.cpu_percent(interval=0.5)
                mem_percent = proc.memory_percent()
                io = proc.io_counters()
                read_mb = io.read_bytes / (1024 * 1024)
                write_mb = io.write_bytes / (1024 * 1024)
                
                # Bars
                cpu_bar = "█" * int(cpu / 10) + "░" * (10 - int(cpu / 10))
                mem_bar = "█" * int(mem_percent / 10) + "░" * (10 - int(mem_percent / 10))
                
                # Status emoji
                status = "🟢" if cpu < 50 else "🟡" if cpu < 80 else "🔴"
                
                print(f"\n{status} {name:<20} [PID: {pid}]")
                print(f"   CPU  {cpu:5.1f}% [{cpu_bar}]")
                print(f"   RAM  {mem_percent:5.2f}% [{mem_bar}]")
                print(f"   I/O  ⬇️ {read_mb:7.1f} MB  ⬆️ {write_mb:7.1f} MB")
                
            except psutil.NoSuchProcess:
                print(f"\n❌ {name:<20} [PID: {pid}] - Process Dead")
        
        print(f"\n{'='*70}")
        time.sleep(10)