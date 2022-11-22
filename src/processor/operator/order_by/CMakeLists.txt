add_library(kuzu_processor_operator_order_by
        OBJECT
        key_block_merger.cpp
        order_by.cpp
        order_by_key_encoder.cpp
        order_by_merge.cpp
        order_by_scan.cpp
        radix_sort.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_order_by>
        PARENT_SCOPE)
