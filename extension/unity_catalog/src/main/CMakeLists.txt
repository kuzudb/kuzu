add_library(kuzu_unity_catalog_extension
        OBJECT
        unity_catalog_extension.cpp)

set(UNITY_CATALOG_EXTENSION_OBJECT_FILES
        ${UNITY_CATALOG_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_unity_catalog_extension>
        PARENT_SCOPE)
