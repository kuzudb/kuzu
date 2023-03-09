eKnows.csv
# This graph has 6000 person nodes (we abbreviate person to p below)
# node p0: has edges to all 5K p's including a self loop.
# nodes p1-p5000 have edges to p5000.
# p5001-p6000: have no incoming or outgoing edges
# This structures ensures the following:
# p0 has a large forward list and 1-edge backward list.
# p5000 has: a large backward list and 1-edge forward list.
# p1-p4999: have small forward, small backward lists.
# p5001-p6000: are singletons (empty forward and backward lists).
# This is a MANY-TO-MANY relationship. Because there are < 65536 person nodes,
# knows edges should use 2 bytes/edge: 0 byte for label and 2 bytes for offset.
# Finally: The edges have an INT64 property with key int64Prop. For any edge
# whose source is p0, the value is the ID of the destination node, e.g., e=0->5 has
# int64Prop:5. The only exceptions are 0->0, 0->1000, 0->2000,... 0->5000. These 6
# edges have null int64prop values. For others the property is null.
# This property is intended to test small and large property list reading.

