add_library(kuzu_gds_config
        OBJECT
        connected_components_config.cpp)

set(GDS_EXTENSION_OBJECT_FILES
        ${GDS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_gds_config>
        PARENT_SCOPE)
