add_library(kuzu_common_data_chunk
        OBJECT
        data_chunk.cpp
        data_chunk_collection.cpp
        data_chunk_state.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_data_chunk>
        PARENT_SCOPE)
