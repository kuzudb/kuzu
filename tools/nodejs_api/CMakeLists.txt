add_definitions(-DNAPI_VERSION=6)
add_definitions(-DNODE_RUNTIME=node)
add_definitions(-DBUILDING_NODE_EXTENSION)

# If on Windows use npx.cmd instead of npx
if(WIN32)
  set(NPX_CMD npx.cmd)
else()
  set(NPX_CMD npx)
endif()

execute_process(
  COMMAND ${NPX_CMD} cmake-js print-cmakejs-include
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE CMAKE_JS_INC
)
execute_process(
  COMMAND ${NPX_CMD} cmake-js print-cmakejs-lib
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE CMAKE_JS_LIB
)
execute_process(
  COMMAND ${NPX_CMD} cmake-js print-cmakejs-src
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE CMAKE_JS_SRC
)

string(STRIP ${CMAKE_JS_INC} CMAKE_JS_INC)
string(STRIP ${CMAKE_JS_LIB} CMAKE_JS_LIB)
string(STRIP ${CMAKE_JS_SRC} CMAKE_JS_SRC)

# Print CMAKE_JS variables
message(STATUS "CMake.js configurations: LIB=${CMAKE_JS_LIB}, INC=${CMAKE_JS_INC}, SRC=${CMAKE_JS_SRC}")

get_filename_component(NODE_ADDON_API_INCLUDE_PATH ./node_modules/node-addon-api ABSOLUTE)

include_directories(${CMAKE_JS_INC})
include_directories(${NODE_ADDON_API_INCLUDE_PATH})

file(GLOB CPP_SOURCE_FILES ./src_cpp/*)
file(GLOB JS_SOURCE_FILES 
    ./src_js/*.js
    ./src_js/*.mjs
    ./src_js/*.d.ts
)
# Copy all JS/TS files to build directory
file(COPY ${JS_SOURCE_FILES} DESTINATION  "${CMAKE_CURRENT_SOURCE_DIR}/build")

add_library(kuzujs SHARED ${CPP_SOURCE_FILES} ${CMAKE_JS_SRC})
set_target_properties(kuzujs PROPERTIES PREFIX "" SUFFIX ".node")
set_target_properties(kuzujs
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build"
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build")
if(APPLE)
  target_link_options(kuzujs PRIVATE -undefined dynamic_lookup)
endif()
target_link_libraries(kuzujs PRIVATE kuzu ${CMAKE_JS_LIB})
