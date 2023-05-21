add_library(
        kuzu_binder_visitor
        OBJECT
        property_collector.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_binder_visitor>
        PARENT_SCOPE)
