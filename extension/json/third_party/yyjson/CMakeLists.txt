add_library(yyjson
    STATIC
    src/yyjson.c)

target_include_directories(yyjson PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})
