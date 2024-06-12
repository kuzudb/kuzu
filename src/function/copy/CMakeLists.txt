add_library(kuzu_function_copy
        OBJECT
        copy_csv_function.cpp
        copy_parquet_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_copy>
        PARENT_SCOPE)
