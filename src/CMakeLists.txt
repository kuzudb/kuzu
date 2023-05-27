add_subdirectory(binder)
add_subdirectory(c_api)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(expression_evaluator)
add_subdirectory(function)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(planner)
add_subdirectory(processor)
add_subdirectory(storage)
add_subdirectory(transaction)

add_library(kuzu STATIC ${ALL_OBJECT_FILES})
target_link_libraries(kuzu
        PUBLIC antlr4_cypher antlr4_runtime utf8proc re2 ${PARQUET_LIB} ${ARROW_LIB} Threads::Threads)
target_include_directories(kuzu
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
add_library(kuzu_shared SHARED ${ALL_OBJECT_FILES})
if(WIN32)
        set_target_properties(kuzu_shared PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
else()
        set_target_properties(kuzu_shared PROPERTIES OUTPUT_NAME kuzu)
endif()
target_link_libraries(kuzu_shared
        PUBLIC antlr4_cypher antlr4_runtime utf8proc re2 ${PARQUET_LIB} ${ARROW_LIB} Threads::Threads)
target_include_directories(kuzu_shared
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
