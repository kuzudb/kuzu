add_library(kuzu_hyperloglog STATIC hyperloglog.cpp sds.cpp)

target_include_directories(kuzu_hyperloglog PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR})
