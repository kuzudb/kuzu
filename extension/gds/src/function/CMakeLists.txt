add_library(kuzu_gds_function
        OBJECT
        component_ids.cpp
        strongly_connected_components.cpp
        strongly_connected_components_kosaraju.cpp
        weakly_connected_components.cpp
        page_rank.cpp
        k_core_decomposition.cpp)

set(GDS_EXTENSION_OBJECT_FILES
        ${GDS_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_gds_function>
        PARENT_SCOPE)
