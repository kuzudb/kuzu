add_library(kuzu_parser_query
        OBJECT
        single_query.cpp)

#target_include_directories(parser_query
#        PUBLIC ../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_query>
        PARENT_SCOPE)
