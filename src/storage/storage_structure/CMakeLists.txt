add_library(kuzu_storage_structure
        OBJECT
        disk_array.cpp
        disk_array_collection.cpp
        overflow_file.cpp
        in_mem_page.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_structure>
        PARENT_SCOPE)
