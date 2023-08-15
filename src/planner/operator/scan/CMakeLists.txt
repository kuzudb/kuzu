add_library(kuzu_planner_scan
        OBJECT
        logical_expressions_scan.cpp
        logical_scan_node.cpp
        logical_scan_node_property.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_scan>
        PARENT_SCOPE)
