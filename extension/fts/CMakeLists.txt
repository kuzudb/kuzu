include_directories(
        ${PROJECT_SOURCE_DIR}/src/include
        src/include
        third_party/snowball/libstemmer)

add_subdirectory(src)

add_library(fts_extension
        SHARED
        ${FTS_OBJECT_FILES})

add_subdirectory(third_party/snowball)

target_link_libraries(fts_extension
        PRIVATE
        snowball)

set_extension_properties(fts_extension fts)

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
    target_link_libraries(fts_extension PRIVATE kuzu)
endif ()

if (APPLE)
    set_apple_dynamic_lookup(fts_extension)
endif ()
