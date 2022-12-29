add_library(kuzu_storage_lists
        OBJECT
        list_headers.cpp
        list_handle.cpp
        lists.cpp
        lists_metadata.cpp
        lists_update_iterator.cpp
        lists_update_store.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_lists>
        PARENT_SCOPE)
