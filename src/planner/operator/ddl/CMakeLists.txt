add_library(kuzu_planner_ddl
        OBJECT
        logical_ddl.cpp
        logical_drop.cpp
        base_logical_create_table.cpp
        base_logical_alter.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_ddl>
        PARENT_SCOPE)