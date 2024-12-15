add_subdirectory(call)

add_library(kuzu_table
        OBJECT
        bind_data.cpp
        simple_table_functions.cpp
        scan_functions.cpp
        bind_input.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table>
        PARENT_SCOPE)
