add_library(kuzu_storage_compression
        OBJECT
        compression.cpp
        bitpacking_int128.cpp
        bitpacking_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_compression>
        PARENT_SCOPE)

target_link_libraries(kuzu_storage_compression PRIVATE fastpfor)
