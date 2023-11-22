add_library(kuzu_function_arithmetic
        OBJECT
        multiply.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_arithmetic>
        PARENT_SCOPE)
