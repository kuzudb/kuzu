# Disable WERROR for gtest.
set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)

set(DOWNLOAD_GTEST TRUE)
if(${PREFER_SYSTEM_DEPS})
    find_package(GTest QUIET)
    if(GTest_FOUND)
        message(STATUS "Using system GTest")
        set(DOWNLOAD_GTEST FALSE)
    endif()
endif()
if (${DOWNLOAD_GTEST})
    message(STATUS "Fetching GTest from GitHub...")
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.17.0
        GIT_SHALLOW    TRUE
    )
    if(WIN32)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    endif()
    FetchContent_MakeAvailable(googletest)
endif()

# Make gtest available to other directories.
add_library(GTEST_LIB INTERFACE IMPORTED GLOBAL)
target_link_libraries(GTEST_LIB INTERFACE GTest::gtest GTest::gtest_main)
target_link_libraries(GTEST_LIB INTERFACE GTest::gmock GTest::gmock_main)
