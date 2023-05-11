add_subdirectory(antlr4_runtime)
add_subdirectory(antlr4_cypher)
add_subdirectory(utf8proc)
add_subdirectory(pybind11)
add_subdirectory(re2)


include(FetchContent)
FetchContent_Declare(
        glob
        GIT_REPOSITORY https://github.com/p-ranav/glob
        GIT_TAG v0.0.1
)
FetchContent_MakeAvailable(glob)
