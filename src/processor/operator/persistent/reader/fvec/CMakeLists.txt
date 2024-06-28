add_library(kuzu_processor_operator_fvecs_reader
        OBJECT
        fvec_reader.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_fvecs_reader>
        PARENT_SCOPE)
