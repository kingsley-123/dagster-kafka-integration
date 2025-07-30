# Save as: validation\phase5_performance\step1_environment.py
import psutil
import platform
import json
import time
from datetime import datetime

print("=== Phase 5 Step 1: Environment Documentation ===")
print("Documenting system specifications for performance baseline...")
print()

def get_system_specs():
    """Document the test environment specifications"""
    specs = {
        "timestamp": datetime.now().isoformat(),
        "platform": {
            "system": platform.system(),
            "node": platform.node(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor()
        },
        "cpu": {
            "physical_cores": psutil.cpu_count(logical=False),
            "total_cores": psutil.cpu_count(logical=True),
            "max_frequency": f"{psutil.cpu_freq().max:.2f} Mhz" if psutil.cpu_freq() else "Unknown",
            "current_frequency": f"{psutil.cpu_freq().current:.2f} Mhz" if psutil.cpu_freq() else "Unknown"
        },
        "memory": {
            "total": f"{psutil.virtual_memory().total / (1024**3):.2f} GB",
            "available": f"{psutil.virtual_memory().available / (1024**3):.2f} GB",
            "percent_used": f"{psutil.virtual_memory().percent}%"
        },
        "disk": {
            "total": f"{psutil.disk_usage('/').total / (1024**3):.2f} GB",
            "free": f"{psutil.disk_usage('/').free / (1024**3):.2f} GB",
            "percent_used": f"{psutil.disk_usage('/').percent}%"
        }
    }
    return specs

try:
    system_specs = get_system_specs()
    
    print("SYSTEM SPECIFICATIONS:")
    print(f"Platform: {system_specs['platform']['system']} {system_specs['platform']['release']}")
    print(f"Processor: {system_specs['platform']['processor']}")
    print(f"CPU Cores: {system_specs['cpu']['physical_cores']} physical, {system_specs['cpu']['total_cores']} logical")
    print(f"Max CPU Frequency: {system_specs['cpu']['max_frequency']}")
    print(f"Total Memory: {system_specs['memory']['total']}")
    print(f"Available Memory: {system_specs['memory']['available']}")
    print(f"Total Disk: {system_specs['disk']['total']}")
    print(f"Free Disk: {system_specs['disk']['free']}")
    print()
    
    # Save specs to file for reference
    import os
    os.makedirs('validation/phase5_performance', exist_ok=True)
    with open('validation/phase5_performance/system_specs.json', 'w') as f:
        json.dump(system_specs, f, indent=2)
    
    print("PASS: System specifications documented and saved to system_specs.json")
    print("READY: Environment baseline established for performance testing")
    
except Exception as e:
    print(f"FAIL: Could not document system specifications: {e}")
    
print()
print("Step 1 complete.")