include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include)

add_library(sample
        SHARED
        src/sample_extension.cpp)

set_target_properties(sample PROPERTIES
        OUTPUT_NAME sample
        PREFIX "lib"
        SUFFIX ".kuzu_extension"
)

set_target_properties(sample
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
)

if (WIN32)
        # On windows, there is no dynamic lookup available, so it's not
        # possible to generically look for symbols on library load. There are
        # two main alternatives to statically linking kuzu, neither of which is
        # appealing:
        #       1. Link against the shared library. This works well assuming
        #       the DLL is locatable, but this assumption isn't valid for users
        #       of kuzu.exe.
        #       2. Link against the executable (kuzu.exe). This is
        #       strange but works well for kuzu.exe. However, it forces
        #       users who are embedding kuzu in their application to recompile
        #       the extension _and_ export the symbols for the extension to
        #       locate on load.
        # We choose the simplest option. Windows isn't known
        # for its small libraries anyways...
        # Future work could make it possible to embed extension into kuzu,
        # which would help fix this problem.
        target_link_libraries(sample PRIVATE kuzu)
endif()

if (APPLE)
    set_target_properties(sample PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()

if (${BUILD_EXTENSION_TESTS})
    add_subdirectory(test)
endif()
