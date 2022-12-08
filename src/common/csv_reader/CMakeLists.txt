add_library(kuzu_common_csv_reader
        OBJECT
        csv_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_csv_reader>
        PARENT_SCOPE)
