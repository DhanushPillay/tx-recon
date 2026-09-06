import hashlib
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


def fingerprint(info=None):
    info = info or get_hardware_info()
    # ponytail: system/machine hang together; OS patch level is
    # deliberately excluded so a routine update does not void the baseline.
    stable = {
        "system": platform.system(),
        "machine": platform.machine(),
        "cpu_count": info["cpu_count"],
        "ram_gb": info["ram_gb"],
    }
    digest = hashlib.sha1(repr(sorted(stable.items())).encode()).hexdigest()
    return digest[:12]
