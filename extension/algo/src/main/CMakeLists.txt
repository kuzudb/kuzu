add_library(algo_extension_main
        OBJECT
        algo_extension.cpp
        )

set(ALGO_EXTENSION_OBJECT_FILES
        ${ALGO_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:algo_extension_main>
        PARENT_SCOPE)
