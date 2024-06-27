add_library(kuzu_function_lambda
        OBJECT
        lambda_function_bind_data.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_lambda>
        PARENT_SCOPE)
