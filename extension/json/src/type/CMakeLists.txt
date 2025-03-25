add_library(kuzu_json_type
        OBJECT
        json_type.cpp)

set(JSON_EXTENSION_OBJECT_FILES
        ${JSON_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_type>
        PARENT_SCOPE)
