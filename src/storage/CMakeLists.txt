add_subdirectory(buffer_manager)
add_subdirectory(compression)
add_subdirectory(local_storage)
add_subdirectory(predicate)
add_subdirectory(index)
add_subdirectory(stats)
add_subdirectory(storage_structure)
add_subdirectory(store)
add_subdirectory(wal)

add_library(kuzu_storage
        OBJECT
        db_file_id.cpp
        file_handle.cpp
        shadow_utils.cpp
        storage_manager.cpp
        storage_utils.cpp
        storage_version_info.cpp
        undo_buffer.cpp
        wal_replayer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage>
        PARENT_SCOPE)
