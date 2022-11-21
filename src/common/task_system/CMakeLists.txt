add_library(kuzu_common_task_system
        OBJECT
        task.cpp
        task_scheduler.cpp)

#target_include_directories(task_system PUBLIC ../../include)
#target_link_libraries(task_system PUBLIC spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_task_system>
        PARENT_SCOPE)
