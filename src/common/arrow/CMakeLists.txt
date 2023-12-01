add_library(kuzu_common_arrow
        OBJECT
        arrow_row_batch.cpp
        arrow_converter.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_arrow>
        PARENT_SCOPE)
