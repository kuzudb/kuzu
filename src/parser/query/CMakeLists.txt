add_subdirectory(reading_clause)

add_library(kuzu_parser_query
        OBJECT
        single_query.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_query>
        PARENT_SCOPE)
