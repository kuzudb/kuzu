add_library(kuzu_processor_operator_rdf_reader
        OBJECT
        rdf_reader.cpp
        rdf_scan.cpp
        rdf_utils.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_rdf_reader>
        PARENT_SCOPE)
