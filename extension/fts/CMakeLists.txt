include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        third_party/snowball/libstemmer)

add_subdirectory(src)
add_subdirectory(test)

add_library(fts_extension
        SHARED
        ${FTS_OBJECT_FILES})

add_subdirectory(third_party/snowball)

target_link_libraries(fts_extension
        PRIVATE
        snowball
        re2)

set_extension_properties(fts_extension fts fts)

if (WIN32)
    # See comment in extension/httpfs/CMakeLists.txt
    target_link_libraries(fts_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(fts_extension)
endif ()
