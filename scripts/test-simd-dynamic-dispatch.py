import gdb


class BreakpointChecker(gdb.Breakpoint):
    def __init__(self, function_name):
        super().__init__(function_name, gdb.BP_BREAKPOINT, internal=False)
        self.function_name = function_name
        self.hit = False

    def stop(self):
        print(f"Breakpoint hit at {self.function_name}")
        self.hit = True
        return False  # Continue execution


# Ensure that an executable was provided
args_output = gdb.execute("show args", to_string=True).strip()
if "Argument list to give program being debugged when it is started is" in args_output:
    args = args_output.split("is ")[-1].strip()
else:
    args = ""

# Ensure an executable was provided
if not args:
    print(
        "Error: No executable specified. Run GDB with --args ./your_executable [args]"
    )
    gdb.execute("quit")
    raise RuntimeError("No executable specified")

# Set the breakpoint before running the program
bp = BreakpointChecker("simsimd_l2_f32_skylake")

# Run the program
gdb.execute("run", to_string=True)  # Capture output to avoid GDB errors propagating

# Check if the breakpoint was hit
if not bp.hit:
    print("Error: Breakpoint was not hit before program terminated.")
    gdb.execute("quit")
    raise RuntimeError("Breakpoint was not hit")

print("Success: Breakpoint was hit")
