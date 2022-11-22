add_library(kuzu_processor_operator_copy_csv
        OBJECT
        copy_node_csv.cpp
        copy_rel_csv.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_copy_csv>
        PARENT_SCOPE)
