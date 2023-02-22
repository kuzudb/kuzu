add_library(kuzu_optimizer
        OBJECT
        factorization_rewriter.cpp
        index_nested_loop_join_optimizer.cpp
        optimizer.cpp
        remove_factorization_rewriter.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_optimizer>
        PARENT_SCOPE)
