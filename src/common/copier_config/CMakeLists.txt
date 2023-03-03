add_library(kuzu_common_copier_config
        OBJECT
        copier_config.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_copier_config>
        PARENT_SCOPE)
