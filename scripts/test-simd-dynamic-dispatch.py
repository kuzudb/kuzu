import gdb

bp = gdb.Breakpoint("simsimd_l2_f32_skylake")

gdb.execute("run")

while True:
    try:
        gdb.execute("continue")
    except gdb.error:
        # the program has terminated
        break

# Check if the breakpoint was hit
if bp.hit_count == 0:
    print("Error: Breakpoint was not hit before program terminated.")
    gdb.execute("quit")
    raise RuntimeError("Breakpoint was not hit")

print("Success: Breakpoint was hit")
gdb.execute("quit")
