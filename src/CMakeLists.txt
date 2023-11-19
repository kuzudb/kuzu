include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Have to pass this down to every subdirectory, which actually adds the files.
# This doesn't affect parent directories.
add_compile_definitions(KUZU_EXPORTS)
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
add_library(kuzu_shared SHARED ${ALL_OBJECT_FILES})

set(KUZU_LIBRARIES antlr4_cypher antlr4_runtime fast_float utf8proc re2 serd Threads::Threads fastpfor miniparquet zstd miniz)
target_link_libraries(kuzu PUBLIC ${KUZU_LIBRARIES})
target_link_libraries(kuzu_shared PUBLIC ${KUZU_LIBRARIES})
unset(KUZU_LIBRARIES)

set(KUZU_INCLUDES $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}> ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(kuzu PUBLIC ${KUZU_INCLUDES})
target_include_directories(kuzu_shared PUBLIC ${KUZU_INCLUDES})
unset(KUZU_INCLUDES)

if(WIN32)
        # Anything linking against the static library must not use dllimport.
        target_compile_definitions(kuzu INTERFACE KUZU_STATIC_DEFINE)
endif()

if(NOT WIN32)
        set_target_properties(kuzu_shared PROPERTIES OUTPUT_NAME kuzu)
endif()

install(TARGETS kuzu kuzu_shared)
