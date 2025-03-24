add_library(sqlite_extension_main
        OBJECT
        sqlite_extension.cpp)

set(SQLITE_EXTENSION_OBJECT_FILES
        ${SQLITE_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:sqlite_extension_main>
        PARENT_SCOPE)
