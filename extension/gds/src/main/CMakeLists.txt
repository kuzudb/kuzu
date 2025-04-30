add_library(gds_extension_main
        OBJECT
        gds_extension.cpp
        in_mem_graph.cpp
        in_mem_gds_utils.cpp
        )

set(GDS_EXTENSION_OBJECT_FILES
        ${GDS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:gds_extension_main>
        PARENT_SCOPE)
