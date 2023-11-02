include(FetchContent)

# Disable WERROR for gtest
if(ENABLE_WERROR)
        set(CMAKE_COMPILE_WARNING_AS_ERROR FALSE)
endif()
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.13.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)
target_link_libraries(GTest::GTest INTERFACE gmock_main)
if(ENABLE_WERROR)
        set(CMAKE_COMPILE_WARNING_AS_ERROR TRUE)
endif()

enable_testing()
add_subdirectory(test_helper)
add_subdirectory(test_runner)
add_subdirectory(graph_test)
add_subdirectory(c_api)
add_subdirectory(common)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(runner)
add_subdirectory(storage)
add_subdirectory(transaction)
add_subdirectory(util_tests)
