add_library(kuzu_main
        OBJECT
        client_context.cpp
        connection.cpp
        database.cpp
        plan_printer.cpp
        prepared_statement.cpp
        query_result.cpp
        query_summary.cpp
        storage_driver.cpp
        db_config.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_main>
        PARENT_SCOPE)
