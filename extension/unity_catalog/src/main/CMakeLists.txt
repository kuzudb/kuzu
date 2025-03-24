add_library(unity_catalog_extension_main
        OBJECT
        unity_catalog_extension.cpp)

set(UNITY_CATALOG_EXTENSION_OBJECT_FILES
        ${UNITY_CATALOG_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:unity_catalog_extension_main>
        PARENT_SCOPE)
