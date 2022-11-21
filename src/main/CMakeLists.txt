add_library(kuzu_main
        OBJECT
        connection.cpp
        database.cpp
        plan_printer.cpp
        query_result.cpp)

#target_include_directories(kuzu_lib PUBLIC ../include)

#target_link_libraries(kuzu_lib PUBLIC spdlog::spdlog)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_main>
        PARENT_SCOPE)
