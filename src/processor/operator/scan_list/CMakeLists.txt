add_library(kuzu_processor_operator_scan_list
        OBJECT
        adj_list_extend.cpp
        scan_rel_property_list.cpp)

#target_include_directories(operator_scan_list PUBLIC ../../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_scan_list>
        PARENT_SCOPE)
