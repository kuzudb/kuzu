add_library(kuzu_sqlite_extension
        OBJECT
        sqlite_extension.cpp)

set(SQLITE_EXTENSION_OBJECT_FILES
        ${SQLITE_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_sqlite_extension>
        PARENT_SCOPE)
