add_library(kuzu_processor_operator_update
        OBJECT
        create.cpp
        delete.cpp
        delete_executor.cpp
        insert_executor.cpp
        set.cpp
        set_executor.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_update>
        PARENT_SCOPE)
