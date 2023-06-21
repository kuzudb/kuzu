add_library(kuzu_comparison_function
        OBJECT
        comparison_operations.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_comparison_function>
        PARENT_SCOPE)
