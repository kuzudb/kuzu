import gdb
import subprocess


def get_machine_architecture():
    output = subprocess.check_output(["cat", "/proc/cpuinfo"])
    flags_str = ""
    for line in output.decode("utf-8").split("\n"):
        line = line.strip()
        if line.startswith("flags"):
            flags_str = line
            break
    assert len(line) > 0

    # Uses the same way as _simsimd_capabilities_x86() in simsimd.h to detect supported features
    # The only simsimd functions we use in the vector index are cos/l2/l2_sq/dot which only branch for the targets 'skylake', 'haswell', and 'serial' so we only test for those
    if "avx512f" in flags_str:
        return "skylake"
    elif "avx2" in flags_str and "f16c" in flags_str and "fma" in flags_str:
        return "haswell"
    else:
        return "serial"


bp = gdb.Breakpoint(f"simsimd_l2_f32_{get_machine_architecture()}")

gdb.execute("run < scripts/simd-dispatch-test.cypher")

try:
    gdb.execute("continue")
    # we only care if the breakpoint is hit at all
    # disable it now to prevent the test from needing to execute 'continue' many times to reach completion
    bp.enabled = False
except gdb.error:
    # the program has terminated
    pass

# Check if the breakpoint was hit
if bp.hit_count == 0:
    print(
        f"Error: did not hit the expected simsimd function for machine architecture '{get_machine_architecture()}'"
    )
    gdb.execute("quit 1")

gdb.execute("quit")
