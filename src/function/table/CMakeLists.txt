add_subdirectory(call)

add_library(kuzu_table
        OBJECT
        call_functions.cpp
        scan_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table>
        PARENT_SCOPE)
