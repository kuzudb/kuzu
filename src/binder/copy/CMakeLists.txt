add_library(
        kuzu_binder_copy
        OBJECT
        bound_copy_from.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_copy>
        PARENT_SCOPE)
