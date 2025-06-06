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
        mbedtls
        OpenSSL::SSL
)
