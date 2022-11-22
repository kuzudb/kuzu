add_library(kuzu_planner_operator
        OBJECT
        base_logical_operator.cpp
        logical_aggregate.cpp
        logical_distinct.cpp
        logical_intersect.cpp
        logical_order_by.cpp
        logical_plan.cpp
        logical_plan_util.cpp
        logical_projection.cpp
        logical_union.cpp
        schema.cpp
        sink_util.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_operator>
        PARENT_SCOPE)
