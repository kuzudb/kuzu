import gdb

bp = gdb.Breakpoint("simsimd_l2_f32_haswell")

gdb.execute("run")

while True:
    try:
        gdb.execute("continue")
        bp.enabled = False
    except gdb.error:
        # the program has terminated
        break

# Check if the breakpoint was hit
if bp.hit_count == 0:
    print("Error: Breakpoint was not hit before program terminated.")
    gdb.execute('quit 1')

print("Success: Breakpoint was hit")
gdb.execute("quit")
