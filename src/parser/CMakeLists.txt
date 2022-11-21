add_subdirectory(antlr_parser)
add_subdirectory(expression)
add_subdirectory(query)

add_library(kuzu_parser
        OBJECT
        parser.cpp
        transformer.cpp)

#target_include_directories(parser
#        PUBLIC ../include
#        PUBLIC ../../third_party/antlr4_runtime/src)

#target_link_libraries(parser PRIVATE antlr4_runtime)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_parser>
        PARENT_SCOPE)
