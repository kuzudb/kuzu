SET(CMAKE_FIND_PACKAGE_SORT_ORDER NATURAL)
SET(CMAKE_FIND_PACKAGE_SORT_DIRECTION DEC)
# Address sanitizer may change the ABI, so we use dynamic linking
# if it's enabled.
if (${ENABLE_ADDRESS_SANITIZER})
    SET(OPENSSL_USE_STATIC_LIBS FALSE)
else ()
    SET(OPENSSL_USE_STATIC_LIBS TRUE)
endif ()

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)
include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        ${PROJECT_SOURCE_DIR}/third_party/httplib
        src/include)

add_library(httpfs_extension_source
        OBJECT
        src/httpfs.cpp
        src/httpfs_extension.cpp
        src/s3fs.cpp
        src/crypto.cpp
        src/http_config.cpp
        src/cached_file_manager.cpp
        src/s3fs_config.cpp)

set(HTTPFS_EXTENSION_OBJECT_FILES
        ${HTTPFS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:httpfs_extension_source>)

build_extension_lib(${BUILD_STATIC_EXTENSION} "httpfs")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        mbedtls
        ${OPENSSL_LIBRARIES})

if (${BUILD_EXTENSION_TESTS})
    add_subdirectory(test)
endif ()
