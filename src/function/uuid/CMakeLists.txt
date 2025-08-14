add_library(kuzu_function_uuid
        OBJECT
        gen_random_uuid.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_uuid>
        PARENT_SCOPE)
