add_subdirectory(buffer_manager)
add_subdirectory(copy_arrow)
add_subdirectory(in_mem_storage_structure)
add_subdirectory(index)
add_subdirectory(storage_structure)
add_subdirectory(store)
add_subdirectory(wal)

add_library(kuzu_storage
        OBJECT
        storage_manager.cpp
        storage_utils.cpp
        wal_replayer.cpp
        wal_replayer_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage>
        PARENT_SCOPE)
