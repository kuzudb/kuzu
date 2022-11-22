add_library(kuzu_storage_wal
        OBJECT
        wal.cpp
        wal_record.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_storage_wal>
        PARENT_SCOPE)
