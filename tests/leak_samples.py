"""Forbidden-looking samples for the leak checks, assembled at run time.

No file in the repository may hold a workspace id, token, host or GUID, not even a
made-up one. Each sample is joined here from harmless parts, so a scan of the committed
text finds none of them. Tests import from this module and write what they need into
their own temporary directory.
"""

SAMPLES = {
    "workspace-id": "adb" + "-" + "0" * 16,
    "access-token": "dap" + "i" + "0123abcd",
    "workspace-host": "example." + "azure" + "databricks" + "." + "net",
    "guid": "-".join(["0" * 8, "0" * 4, "0" * 4, "0" * 4, "0" * 12]),
}

HOST_FRAGMENT = "azure" + "databricks" + "." + "net"

LEAK_ORDER = ["workspace-id", "access-token", "workspace-host", "guid"]

CLEAN_HEAD = "GATE: PASS checks=6\n"


def leak_output_text():
    """A run output that holds every sample once, one per line, in LEAK_ORDER."""
    lines = [CLEAN_HEAD.rstrip("\n")]
    lines += ["note %s end" % SAMPLES[name] for name in LEAK_ORDER]
    return "\n".join(lines) + "\n"
