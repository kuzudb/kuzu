add_library(kuzu_algo_config
        OBJECT
        connected_components_config.cpp)

set(ALGO_EXTENSION_OBJECT_FILES
        ${ALGO_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_algo_config>
        PARENT_SCOPE)
