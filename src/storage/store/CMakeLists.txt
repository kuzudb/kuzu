add_library(kuzu_storage_store
        OBJECT
        node_table.cpp
        nodes_statistics_and_deleted_ids.cpp
        nodes_store.cpp
        rel_table.cpp
        rels_statistics.cpp
        rels_store.cpp
        table_statistics.cpp)

#target_include_directories(store PUBLIC ../../include)
#target_link_libraries(store PRIVATE spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_store>
        PARENT_SCOPE)
