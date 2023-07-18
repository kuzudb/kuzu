add_library(kuzu_planner_plan_operator
        OBJECT
        append_accumulate.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_plan_operator>
        PARENT_SCOPE)
