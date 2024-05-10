add_library(kuzu_processor_operator_ddl
        OBJECT
        alter.cpp
        create_table.cpp
        ddl.cpp
        drop_table.cpp
        create_sequence.cpp
        drop_sequence.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ddl>
        PARENT_SCOPE)
