add_library(kuzu_reading_clause
        OBJECT
        in_query_call_clause.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_reading_clause>
        PARENT_SCOPE)
