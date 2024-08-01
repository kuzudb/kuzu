add_library(kuzu_planner_simple
        OBJECT
        logical_simple.cpp
        logical_create_table.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_simple>
        PARENT_SCOPE)
