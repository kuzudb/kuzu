add_subdirectory(join_order)
add_subdirectory(operator)
add_subdirectory(plan)

add_library(kuzu_planner
        OBJECT
        planner.cpp
        query_planner.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_planner>
        PARENT_SCOPE)
