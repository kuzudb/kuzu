add_library(kuzu_json_main
        OBJECT
        json_extension.cpp)

set(JSON_EXTENSION_OBJECT_FILES
        ${JSON_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_main>
        PARENT_SCOPE)
