add_library(kuzu_json_extract_functions
        OBJECT
        json_extract.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_extract_functions>
        PARENT_SCOPE)
