add_library(kuzu_parser_antlr_parser
        OBJECT
        kuzu_cypher_parser.cpp
        parser_error_listener.cpp
        parser_error_strategy.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_antlr_parser>
        PARENT_SCOPE)
