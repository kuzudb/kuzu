add_library(kuzu_duckdb_connector
        OBJECT
        duckdb_connector.cpp
        local_duckdb_connector.cpp
        remote_duckdb_connector.cpp
        connector_factory.cpp
        duckdb_result_converter.cpp
        duckdb_secret_manager.cpp
        duckdb_type_converter.cpp)

set(DUCKDB_EXTENSION_OBJECT_FILES
        ${DUCKDB_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_duckdb_connector>
        PARENT_SCOPE)
