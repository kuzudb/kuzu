add_library(kuzu_iceberg_main
        OBJECT
        iceberg_extension.cpp)

set(ICEBERG_OBJECT_FILES
        ${ICEBERG_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_iceberg_main>
        PARENT_SCOPE)
