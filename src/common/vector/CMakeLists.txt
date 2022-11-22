add_library(kuzu_common_vector
        OBJECT
        value_vector.cpp
        value_vector_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_vector>
        PARENT_SCOPE)
