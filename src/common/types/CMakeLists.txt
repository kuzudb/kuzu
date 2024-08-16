add_subdirectory(value)

add_library(kuzu_common_types
        OBJECT
        blob.cpp
        date_t.cpp
        dtime_t.cpp
        interval_t.cpp
        ku_list.cpp
        ku_string.cpp
        timestamp_t.cpp
        types.cpp
        int128_t.cpp
        uuid.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_types>
        PARENT_SCOPE)
