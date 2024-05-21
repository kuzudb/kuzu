add_library(kuzu_algorithm_graph
        OBJECT
        demo_algorithm.cpp
        ife_morsel.cpp
        page_rank.cpp
        shortest_path.cpp
        variable_length_path.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_algorithm_graph>
        PARENT_SCOPE)
