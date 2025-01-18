if(${DEFAULT_REL_STORAGE_DIRECTION} STREQUAL "FWD")
        add_definitions(-DKUZU_DEFAULT_REL_STORAGE_DIRECTION=1)
elseif(${DEFAULT_REL_STORAGE_DIRECTION} STREQUAL "BWD")
        add_definitions(-DKUZU_DEFAULT_REL_STORAGE_DIRECTION=2)
else()
        add_definitions(-DKUZU_DEFAULT_REL_STORAGE_DIRECTION=0)
endif()

add_library(kuzu_common_enums
        OBJECT
        accumulate_type.cpp
        path_semantic.cpp
        query_rel_type.cpp
        rel_direction.cpp
        rel_multiplicity.cpp
        scan_source_type.cpp
        table_type.cpp
        transaction_action.cpp
        drop_type.cpp
        extend_direction.cpp
        conflict_action.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_enums>
        PARENT_SCOPE)
