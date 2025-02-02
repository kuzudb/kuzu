add_executable(kuzu_wasm
    src_cpp/main.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/connection.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/database.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/data_type.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/helpers.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/flat_tuple.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/prepared_statement.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/query_result.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/query_summary.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/value.cpp
    ${PROJECT_SOURCE_DIR}/src/c_api/version.cpp
)

file(GLOB JS_SOURCE_FILES ./src_js/*)

target_link_libraries(kuzu_wasm PRIVATE kuzu)

set_target_properties(kuzu_wasm
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/kuzu"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/kuzu"
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/kuzu"
)

file(COPY ${JS_SOURCE_FILES} DESTINATION  "${CMAKE_CURRENT_SOURCE_DIR}/build")
