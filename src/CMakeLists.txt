include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_compile_definitions(KUZU_CMAKE_VERSION=v${CMAKE_PROJECT_VERSION})
# Avoid the import annotation when building on windows
# Really this should be set per target,
# but the targets are split among many files and only the object files are linked against here
add_compile_definitions(kuzu_shared_EXPORTS)

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
        PUBLIC antlr4_cypher antlr4_runtime fast_float utf8proc re2 serd Threads::Threads fastpfor miniparquet zstd miniz)
target_include_directories(kuzu
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
add_library(kuzu_shared SHARED ${ALL_OBJECT_FILES})
if(NOT WIN32)
        set_target_properties(kuzu_shared PROPERTIES OUTPUT_NAME kuzu)
endif()
target_link_libraries(kuzu_shared
        PUBLIC antlr4_cypher antlr4_runtime fast_float utf8proc re2 serd Threads::Threads fastpfor miniparquet zstd miniz)
target_include_directories(kuzu_shared
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

include(GenerateExportHeader)
target_include_directories(kuzu PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(kuzu_shared PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_compile_definitions(kuzu PUBLIC KUZU_STATIC_DEFINE)
