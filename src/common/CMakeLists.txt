add_subdirectory(arrow)
add_subdirectory(copier_config)
add_subdirectory(data_chunk)
add_subdirectory(task_system)
add_subdirectory(types)
add_subdirectory(vector)

add_library(kuzu_common
        OBJECT
        assert.cpp
        rel_direction.cpp
        expression_type.cpp
        file_utils.cpp
        in_mem_overflow_buffer.cpp
        logging_level_utils.cpp
        metric.cpp
        null_mask.cpp
        profiler.cpp
        type_utils.cpp
        utils.cpp
        string_utils.cpp)

target_link_libraries(kuzu_common Glob)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common>
        PARENT_SCOPE)
