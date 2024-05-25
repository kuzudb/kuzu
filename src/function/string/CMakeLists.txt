add_library(kuzu_string_function
        OBJECT
        string_split_function.cpp
        init_cap_function.cpp
        levenshtein_function.cpp
        split_part.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_string_function>
        PARENT_SCOPE)
