add_subdirectory(antlr_parser)
add_subdirectory(expression)
add_subdirectory(transform)
add_subdirectory(visitor)

add_library(kuzu_parser
        OBJECT
        create_macro.cpp
        parsed_expression_visitor.cpp
        parser.cpp
        parsed_statement_visitor.cpp
        transformer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser>
        PARENT_SCOPE)
