add_library(kuzu_utility_function
        OBJECT
        coalesce.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_utility_function>
        PARENT_SCOPE)
