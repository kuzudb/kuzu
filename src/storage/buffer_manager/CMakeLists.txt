add_library(kuzu_storage_buffer_manager
        OBJECT
        buffer_managed_file_handle.cpp
        buffer_manager.cpp
        buffer_pool.cpp
        memory_manager.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_buffer_manager>
        PARENT_SCOPE)
