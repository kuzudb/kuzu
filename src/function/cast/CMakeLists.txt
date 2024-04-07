add_library(kuzu_function_cast
        OBJECT
        cast_rdf_variant.cpp
        cast_array.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_cast>
        PARENT_SCOPE)
