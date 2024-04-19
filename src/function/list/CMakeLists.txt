add_library(kuzu_list_function
        OBJECT
        list_to_string_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_list_function>
        PARENT_SCOPE)
