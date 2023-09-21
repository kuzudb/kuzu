add_library(kuzu_planner_scan
        OBJECT
        logical_expressions_scan.cpp
        logical_index_scan.cpp
        logical_scan_file.cpp
        logical_scan_internal_id.cpp
        logical_scan_node_property.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_scan>
        PARENT_SCOPE)
