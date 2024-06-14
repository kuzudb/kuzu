add_library(kuzu_planner_join_order
        OBJECT
        cardinality_estimator.cpp
        cost_model.cpp
        dp_table.cpp
        join_order_solver.cpp
        join_order_util.cpp
        join_plan_solver.cpp
        join_tree.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner_join_order>
        PARENT_SCOPE)
