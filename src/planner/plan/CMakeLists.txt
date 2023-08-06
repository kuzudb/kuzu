add_library(kuzu_planner_plan_operator
        OBJECT
        append_accumulate.cpp
        append_aggregate.cpp
        append_create.cpp
        append_cross_product.cpp
        append_delete.cpp
        append_limit.cpp
        append_multiplicity_reducer.cpp
        append_order_by.cpp
        append_projection.cpp
        append_set.cpp
        append_skip.cpp
        plan_projection.cpp
        plan_update.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_plan_operator>
        PARENT_SCOPE)
