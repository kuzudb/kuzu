add_library(kuzu_planner_copy
        OBJECT
        logical_copy_from.cpp
        logical_copy_to.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_copy>
        PARENT_SCOPE)
