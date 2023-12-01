include(FetchContent)

# Disable WERROR for gtest.
set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
# Disable LTO for gtest, since it uses an older version of CMake.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.13.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
