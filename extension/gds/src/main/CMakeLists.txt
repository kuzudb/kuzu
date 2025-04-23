add_library(gds_extension_main
        OBJECT
        gds_extension.cpp)

set(GDS_EXTENSION_OBJECT_FILES
        ${GDS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:gds_extension_main>
        PARENT_SCOPE)
