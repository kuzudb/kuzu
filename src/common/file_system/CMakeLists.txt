add_library(kuzu_file_system
        OBJECT
        file_info.cpp
        file_system.cpp
        local_file_system.cpp
        virtual_file_system.cpp)

target_link_libraries(kuzu_file_system Glob)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_file_system>
        PARENT_SCOPE)
