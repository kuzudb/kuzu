add_library(kuzu_binder_bind_copy
        OBJECT
        bind_copy_rdf_graph.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_bind_copy>
        PARENT_SCOPE)
