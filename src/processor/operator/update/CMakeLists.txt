add_library(kuzu_processor_operator_update
        OBJECT
        create.cpp
        delete.cpp
        set.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_update>
        PARENT_SCOPE)
