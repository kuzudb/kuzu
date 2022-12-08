add_subdirectory(lists)

add_library(kuzu_storage_structure
        OBJECT
        column.cpp
        disk_array.cpp
        disk_overflow_file.cpp
        in_mem_file.cpp
        in_mem_page.cpp
        storage_structure.cpp
        storage_structure_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_structure>
        PARENT_SCOPE)
