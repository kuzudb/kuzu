import sys

EXP = 127
OUT_FILE = "dataset/int128-db/vPerson.csv"


def make_high_compression_series(numEntries):
    def high_compression_series(entryIdx):
        HIGHEST = 2**EXP - 1
        return HIGHEST - (numEntries - entryIdx)

    return high_compression_series


def make_low_compression_series(numEntries):
    def low_compression_series(entryIdx):
        diff = entryIdx + 1
        vals = [-(2 ** (EXP - 1) - diff), 2 ** (EXP - 1) - diff]
        return vals[entryIdx % 2] + 2**EXP

    return low_compression_series


def generate_csv(numEntries, generator):
    with open(OUT_FILE, "w") as f_out:
        for i in (x for x in range(numEntries)):
            f_out.write(str(generator(i)) + "\n")


num_entries = int(sys.argv[1])
input_type = sys.argv[2]
print(f"num_entries={num_entries}")
if input_type == "high":
    generate_csv(num_entries, make_high_compression_series(num_entries))
else:
    generate_csv(num_entries, make_low_compression_series(num_entries))
