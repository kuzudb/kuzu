add_library(kuzu_extension
        OBJECT
        catalog_extension.cpp
        extension.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_extension>
        PARENT_SCOPE)
