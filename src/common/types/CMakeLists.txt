add_library(kuzu_common_types
        OBJECT
        date_t.cpp
        dtime_t.cpp
        internal_id_t.cpp
        interval_t.cpp
        ku_list.cpp
        ku_string.cpp
        value.cpp
        timestamp_t.cpp
        types.cpp
        blob.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_types>
        PARENT_SCOPE)
