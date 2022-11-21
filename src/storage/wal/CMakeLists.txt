add_library(kuzu_storage_wal
        OBJECT
        wal.cpp
        wal_record.cpp)

#target_include_directories(wal PUBLIC ../../include)
#target_link_libraries(wal PUBLIC spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_wal>
        PARENT_SCOPE)
