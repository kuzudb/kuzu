add_library(kuzu_parser_expression
        OBJECT
        parsed_expression.cpp)

#target_include_directories(parser_expression
#        PUBLIC ../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_expression>
        PARENT_SCOPE)
