set(EXTENSION_LIST azure delta duckdb fts httpfs iceberg json llm postgres sqlite unity_catalog vector neo4j algo)


if (BUILD_EXTENSION_RELEASE)
    message(STATUS "Building extensions in Release mode, disable extension static linking.")
else()
    set(EXTENSION_STATIC_LINK_LIST algo fts json vector)
endif()
string(JOIN ", " joined_extensions ${EXTENSION_STATIC_LINK_LIST})
message(STATUS "Static link extensions: ${joined_extensions}")
foreach(extension IN LISTS EXTENSION_STATIC_LINK_LIST)
    add_static_link_extension(${extension})
endforeach()

