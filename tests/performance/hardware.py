import os
import platform


def get_hardware_info():
    try:
        import psutil

        ram_gb = round(psutil.virtual_memory().total / (1024**3), 1)
    except ImportError:
        ram_gb = None

    return {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "ram_gb": ram_gb,
        "python_version": platform.python_version(),
    }
