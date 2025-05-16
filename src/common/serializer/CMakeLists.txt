add_library(kuzu_common_serializer
        OBJECT
        buffered_file.cpp
        buffered_serializer.cpp
        deserializer.cpp
        metadata_writer.cpp
        serializer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_serializer>
        PARENT_SCOPE)
