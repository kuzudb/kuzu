add_library(kuzu_fts_function
        OBJECT
        create_fts_index.cpp
        drop_fts_index.cpp
        fts_config.cpp
        fts_index_utils.cpp
        query_fts_index.cpp
        query_fts_bind_data.cpp
        stem.cpp)

set(FTS_EXTENSION_OBJECT_FILES
        ${FTS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_fts_function>
        PARENT_SCOPE)
