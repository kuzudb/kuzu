add_library(kuzu_parser_antlr_parser
        OBJECT
        cypher_lexer.cpp
        cypher_parser.cpp
        kuzu_cypher_parser.cpp
        parser_error_listener.cpp
        parser_error_strategy.cpp)

#target_include_directories(parser_antlr_parser
#        PUBLIC ../../include
#        PRIVATE ../../../third_party/antlr4_runtime/src)

#target_link_libraries(parser_antlr_parser PUBLIC antlr4_runtime)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_antlr_parser>
        PARENT_SCOPE)
