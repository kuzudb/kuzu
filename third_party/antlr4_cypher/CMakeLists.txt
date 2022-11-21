add_library(antlr4_cypher
        STATIC
        cypher_lexer.cpp
        cypher_parser.cpp)

target_include_directories(antlr4_cypher
        PRIVATE ../antlr4_runtime/src)

target_link_libraries(antlr4_cypher PRIVATE antlr4_runtime)
