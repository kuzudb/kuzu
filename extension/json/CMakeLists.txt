include_directories(
        src/include
        ${PROJECT_SOURCE_DIR}/src/include
        third_party/yyjson/src
)

add_subdirectory(src)

add_library(kuzu_json_extension
        SHARED
        ${JSON_OBJECT_FILES}
)

add_subdirectory(third_party)

set_target_properties(kuzu_json_extension PROPERTIES
        OUTPUT_NAME json
        PREFIX "lib"
        SUFFIX ".kuzu_extension"
)

set_target_properties(kuzu_json_extension
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
)

target_link_libraries(kuzu_json_extension
        PRIVATE
        yyjson
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
    target_link_libraries(kuzu_json_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_target_properties(kuzu_json_extension PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif ()
