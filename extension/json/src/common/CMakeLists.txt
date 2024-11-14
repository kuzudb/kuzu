add_library(kuzu_json_common
        OBJECT
        json_common.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_common>
        PARENT_SCOPE)
