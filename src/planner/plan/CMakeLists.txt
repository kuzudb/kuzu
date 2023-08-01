add_library(kuzu_planner_plan_operator
        OBJECT
        append_accumulate.cpp
        append_create.cpp
        append_cross_product.cpp
        append_delete.cpp
        append_set.cpp
        plan_update.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_plan_operator>
        PARENT_SCOPE)
