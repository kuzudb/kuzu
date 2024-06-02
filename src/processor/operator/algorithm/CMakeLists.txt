add_library(kuzu_processor_operator_algorithm
        OBJECT
        algorithm_runner_main.cpp
        algorithm_runner_worker.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_algorithm>
        PARENT_SCOPE)
