add_subdirectory(functions/creation_functions)
add_subdirectory(functions/export_functions)
add_subdirectory(functions/extract_functions)
add_subdirectory(functions/scalar_functions)
add_subdirectory(functions/table_functions)
add_subdirectory(utils)
add_subdirectory(reader)
add_subdirectory(type)
add_subdirectory(common)

add_library(kuzu_json_extension_src
        OBJECT
        json_extension.cpp)

set(JSON_OBJECT_FILES
        ${JSON_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_json_extension_src>
        PARENT_SCOPE)
