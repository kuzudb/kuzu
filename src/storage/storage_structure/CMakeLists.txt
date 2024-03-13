add_library(kuzu_storage_structure
        OBJECT
        disk_array.cpp
        overflow_file.cpp
        in_mem_page.cpp
        db_file_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_structure>
        PARENT_SCOPE)
