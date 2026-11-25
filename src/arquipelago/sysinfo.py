"""Descrição do hardware do nó (processador e núcleos), mostrada no relatório do teste."""

import os
import platform


def cpu_name() -> str:
    if os.name == "nt":
        try:
            import winreg
            key = r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key) as k:
                return " ".join(str(winreg.QueryValueEx(k, "ProcessorNameString")[0]).split())
        except OSError:
            pass
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine() or "processador desconhecido"


def machine_info() -> str:
    return f"{cpu_name()}, {os.cpu_count()} threads"
