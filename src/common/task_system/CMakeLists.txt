add_library(kuzu_common_task_system
        OBJECT
        task.cpp
        task_scheduler.cpp 
        progress_bar.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_task_system>
        PARENT_SCOPE)
