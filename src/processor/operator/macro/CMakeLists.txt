add_library(kuzu_processor_operator_create_macro
        OBJECT
        create_macro.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_create_macro>
        PARENT_SCOPE)
