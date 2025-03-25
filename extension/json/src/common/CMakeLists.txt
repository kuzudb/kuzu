add_library(kuzu_json_common
        OBJECT
        json_common.cpp)

set(JSON_EXTENSION_OBJECT_FILES
        ${JSON_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_common>
        PARENT_SCOPE)
