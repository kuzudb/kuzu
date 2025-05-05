add_subdirectory(config)

add_library(kuzu_algo_function
        OBJECT
        component_ids.cpp
        strongly_connected_components.cpp
        strongly_connected_components_kosaraju.cpp
        weakly_connected_components.cpp
        page_rank.cpp
        k_core_decomposition.cpp
        louvain.cpp
        )

set(ALGO_EXTENSION_OBJECT_FILES
        ${ALGO_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_algo_function>
        PARENT_SCOPE)
