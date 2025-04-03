add_library(kuzu_timestamp_function
        OBJECT
        to_epoch_ms.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_timestamp_function>
        PARENT_SCOPE)
