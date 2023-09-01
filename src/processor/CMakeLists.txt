add_subdirectory(map)
add_subdirectory(operator)
add_subdirectory(result)

add_library(kuzu_processor
        OBJECT
        processor.cpp
        processor_task.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor>
        PARENT_SCOPE)
