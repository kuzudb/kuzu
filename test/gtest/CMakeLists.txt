# Disable WERROR for gtest.
set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)

find_package(GTest QUIET)
if(GTest_FOUND)
    message(STATUS "Using system GTest")
else()
    message(STATUS "GTest not found. Fetching from GitHub...")
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
