add_subdirectory(antlr_parser)
add_subdirectory(expression)
add_subdirectory(query)
add_subdirectory(macro)

add_library(kuzu_parser
        OBJECT
        parser.cpp
        transformer.cpp
        parsed_expression_visitor.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser>
        PARENT_SCOPE)
