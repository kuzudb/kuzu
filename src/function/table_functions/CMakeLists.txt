add_library(kuzu_table_function
        OBJECT
        call_functions.cpp
        scan_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_table_function>
        PARENT_SCOPE)
