add_library(kuzu_sqlite_scan
        OBJECT
        sqlite_scan.cpp)

set(SQLITE_EXTENSION_OBJECT_FILES
        ${SQLITE_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_sqlite_scan>
        PARENT_SCOPE)
