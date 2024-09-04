add_library(kuzu_json_export_functions
        OBJECT
        json_export.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_export_functions>
        PARENT_SCOPE)
