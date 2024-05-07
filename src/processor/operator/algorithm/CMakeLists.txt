add_library(kuzu_processor_operator_algorithm
        OBJECT
        algorithm_runner.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_algorithm>
        PARENT_SCOPE)
