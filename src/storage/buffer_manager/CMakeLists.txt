add_library(kuzu_storage_buffer_manager
        OBJECT
        buffer_manager.cpp
        buffer_pool.cpp
        file_handle.cpp
        memory_manager.cpp
        versioned_file_handle.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_buffer_manager>
        PARENT_SCOPE)
