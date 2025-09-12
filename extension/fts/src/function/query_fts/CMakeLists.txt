add_library(kuzu_query_fts_function
        OBJECT
        query_fts_index.cpp
        query_fts_pattern_match.cpp
        query_fts_bind_data.cpp
        query_fts_term_lookup.cpp)

set(FTS_EXTENSION_OBJECT_FILES
        ${FTS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_query_fts_function>
        PARENT_SCOPE)
