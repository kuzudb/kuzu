add_library(
        kuzu_binder_ddl
        OBJECT
        bound_create_table_info.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_ddl>
        PARENT_SCOPE)