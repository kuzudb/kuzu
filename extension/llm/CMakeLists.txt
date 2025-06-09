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
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)
include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/src/include
        src/include
        ${PROJECT_SOURCE_DIR}/third_party/simsimd/include
        ${OPENSSL_INCLUDE_DIR})




add_subdirectory(src/function)
add_subdirectory(src/main)
add_subdirectory(src/providers)

build_extension_lib(${BUILD_STATIC_EXTENSION} "llm")

target_link_libraries(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        simsimd
        OpenSSL::SSL
        mbedtls
)


target_sources(kuzu_${EXTENSION_LIB_NAME}_extension
        PRIVATE
        ${PROJECT_SOURCE_DIR}/src/common/crypto.cpp
)
