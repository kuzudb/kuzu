add_library(kuzu_common_vector_index
        OBJECT
        helpers.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_vector_index>
        PARENT_SCOPE)
