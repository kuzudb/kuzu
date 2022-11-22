add_library(kuzu_processor_operator_ver_length_extend
        OBJECT
        var_length_adj_list_extend.cpp
        var_length_column_extend.cpp
        var_length_extend.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ver_length_extend>
        PARENT_SCOPE)
