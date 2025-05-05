add_library(algo_extension_common
        OBJECT
        in_mem_graph.cpp
        in_mem_gds_utils.cpp
        )

set(ALGO_EXTENSION_OBJECT_FILES
        ${ALGO_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:algo_extension_common>
        PARENT_SCOPE)
