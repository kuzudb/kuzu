eKnows.csv
# This is a replica of the graph under 2-bytes-per-edge with 2 additional vertex a:0, p:6000 (so there are now 2 different vertex labels)
# and 1 additional edge: a0-[knows]->p6000.
# knows edges should use 3 bytes/edge: 1 byte for label (b/c src/dst labels of knows edges can be animal or person)
# and 2 bytes for offset (b/c the number of p and a nodes are < 65536).
