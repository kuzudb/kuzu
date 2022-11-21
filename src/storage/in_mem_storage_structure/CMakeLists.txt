add_library(kuzu_storage_in_mem_storage_structure
        OBJECT
        in_mem_column.cpp
        in_mem_lists.cpp)

#target_include_directories(in_mem_storage_structure PUBLIC ../../include)
#target_link_libraries(in_mem_storage_structure PUBLIC spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_in_mem_storage_structure>
        PARENT_SCOPE)
