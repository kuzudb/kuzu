add_library(kuzu_planner_persistent
        OBJECT
        logical_create.cpp
        logical_delete.cpp
        logical_merge.cpp
        logical_set.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_persistent>
        PARENT_SCOPE)
