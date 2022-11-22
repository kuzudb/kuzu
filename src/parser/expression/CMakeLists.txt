add_library(kuzu_parser_expression
        OBJECT
        parsed_expression.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_expression>
        PARENT_SCOPE)
