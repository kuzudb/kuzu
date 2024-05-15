add_library(kuzu_function_date
        OBJECT
        date_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_date>
        PARENT_SCOPE)
