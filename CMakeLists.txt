cmake_minimum_required(VERSION 3.11)

project(Kuzu VERSION 0.0.1 LANGUAGES CXX)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_program(CCACHE_PROGRAM ccache)
if (CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
else ()
    find_program(CCACHE_PROGRAM sccache)
    if (CCACHE_PROGRAM)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    endif ()
endif ()

set(INSTALL_LIB_DIR
        lib
        CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR
        bin
        CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR
        include
        CACHE PATH "Installation directory for header files")
set(INSTALL_CMAKE_DIR
        ${DEF_INSTALL_CMAKE_DIR}
        CACHE PATH "Installation directory for CMake files")

option(ENABLE_ADDRESS_SANITIZER "Enable address sanitizer." FALSE)
option(ENABLE_THREAD_SANITIZER "Enable thread sanitizer." FALSE)
option(ENABLE_UBSAN "Enable undefined behavior sanitizer." FALSE)
if(${ENABLE_THREAD_SANITIZER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fno-common -fpermissive")
endif()
if(${ENABLE_ADDRESS_SANITIZER})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fno-common -fpermissive")
endif()
if(${ENABLE_UBSAN})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-common -fpermissive")
endif()

function(link_threads LIBRARY)
    if (CMAKE_VERSION VERSION_LESS "3.1")
        target_link_libraries(${LIBRARY} pthread)
    else ()
        target_link_libraries(${LIBRARY} Threads::Threads)
    endif ()
endfunction()

function(add_kuzu_test TEST_NAME)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PRIVATE test_helper)
    target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include)
    include(GoogleTest)
    gtest_discover_tests(${TEST_NAME})
endfunction()

add_definitions(-DKUZU_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")

include_directories(src/include)
include_directories(third_party/antlr4_cypher/include)
include_directories(third_party/antlr4_runtime/src)
include_directories(third_party/spdlog)
include_directories(third_party/nlohmann_json)
include_directories(third_party/utf8proc/include)

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(tools)
