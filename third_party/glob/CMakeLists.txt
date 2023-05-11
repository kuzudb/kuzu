add_library(Glob INTERFACE glob/glob.hpp)

target_include_directories(Glob INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
