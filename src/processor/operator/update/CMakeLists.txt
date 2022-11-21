add_library(kuzu_processor_operator_update
        OBJECT
        create.cpp
        delete.cpp
        set.cpp)

#target_include_directories(operator_update PUBLIC ../../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_update>
        PARENT_SCOPE)
