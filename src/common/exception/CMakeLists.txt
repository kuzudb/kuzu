add_library(kuzu_common_exception
        OBJECT
        message.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_common_exception>
        PARENT_SCOPE)
