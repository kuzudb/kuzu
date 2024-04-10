add_library(kuzu_binder_bind_read
        OBJECT
        bind_in_query_call.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_bind_read>
        PARENT_SCOPE)
