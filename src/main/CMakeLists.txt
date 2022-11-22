add_library(kuzu_main
        OBJECT
        connection.cpp
        database.cpp
        plan_printer.cpp
        query_result.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_main>
        PARENT_SCOPE)
