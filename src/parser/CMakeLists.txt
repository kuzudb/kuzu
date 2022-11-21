add_subdirectory(antlr_parser)
add_subdirectory(expression)
add_subdirectory(query)

add_library(kuzu_parser
        OBJECT
        parser.cpp
        transformer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser>
        PARENT_SCOPE)
