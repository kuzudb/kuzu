add_library(kuzu_storage_in_mem_csv_copier
        OBJECT
        in_mem_node_csv_copier.cpp
        in_mem_rel_csv_copier.cpp
        in_mem_structures_csv_copier.cpp)

#target_include_directories(in_mem_csv_copier PUBLIC ../../include)
#target_link_libraries(in_mem_csv_copier PRIVATE spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_csv_copier>
        PARENT_SCOPE)
