add_library(kuzu_processor_operator_intersect
        OBJECT
        intersect.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_intersect>
        PARENT_SCOPE)
