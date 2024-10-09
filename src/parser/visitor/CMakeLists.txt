add_library(
        kuzu_parser_visitor
        OBJECT
        standalone_call_analyzer.cpp
        statement_read_write_analyzer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser_visitor>
        PARENT_SCOPE)
