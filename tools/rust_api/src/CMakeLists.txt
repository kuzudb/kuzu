add_library(kuzu_rs STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/kuzu_rs.cpp
    ${KUZU_RS_BINDINGS_DIR}/sources/kuzu/src/lib.rs.cc
)

target_include_directories(kuzu_rs
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
        ${KUZU_RS_BINDINGS_DIR}/include
        ${KUZU_RS_BINDINGS_DIR}/crate
        ${KUZU_RS_BINDINGS_DIR}/sources
)

target_link_libraries(kuzu_rs kuzu)
