add_library(kuzu_function_path
        OBJECT
        length_function.cpp
        nodes_function.cpp
        properties_function.cpp
        rels_function.cpp
        semantic_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_path>
        PARENT_SCOPE)
