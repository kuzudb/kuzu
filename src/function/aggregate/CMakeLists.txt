add_library(kuzu_function_aggregate
        OBJECT
        count.cpp
        count_star.cpp
        collect.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_aggregate>
        PARENT_SCOPE)
