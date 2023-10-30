add_subdirectory(antlr_parser)
add_subdirectory(expression)
add_subdirectory(transform)

add_library(kuzu_parser
        OBJECT
        create_macro.cpp
        parser.cpp
        transformer.cpp
        parsed_expression_visitor.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser>
        PARENT_SCOPE)
