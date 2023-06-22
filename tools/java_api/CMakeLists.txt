cmake_minimum_required (VERSION 3.11)

set(CMAKE_FIND_PACKAGE_RESOLVE_SYMLINKS TRUE)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

project (kuzu_java_api)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()
include(UseJava)

enable_testing()

file(GLOB JAVA_SRC_FILES *.java)

set(CMAKE_JAVA_COMPILE_FLAGS -source 1.8 -target 1.8 -encoding utf-8)
add_jar(kuzu_java ${JAVA_SRC_FILES})
get_target_property(_jarFile kuzu_java JAR_FILE)
get_target_property(_classDir kuzu_java CLASSDIR)

set (_stubDir "${CMAKE_CURRENT_BINARY_DIR}")
add_custom_command(
        OUTPUT KuzuNative.h
        COMMAND ${Java_JAVAC_EXECUTABLE} 
        -h .
        ${JAVA_SRC_FILES}
)

# generate libfoo.jnilib
include_directories(${JNI_INCLUDE_DIRS} ${_classDir} ${_stubDir} ../../src/include)
include_directories(../../third_party/antlr4_cypher/include)
include_directories(../../third_party/antlr4_runtime/src)
include_directories(../../third_party/spdlog)
include_directories(../../third_party/nlohmann_json)
include_directories(../../third_party/pyparse)
include_directories(../../third_party/utf8proc/include)
include_directories(../../third_party/pybind11/include)
include_directories(../../third_party/re2/include)
include_directories(../../third_party/concurrentqueue)

find_library(KUZU NAMES kuzu PATHS ../../build/release/src)

add_library(kuzu_java_native MODULE kuzu_java.cpp KuzuNative.h)
set_target_properties(kuzu_java_native PROPERTIES SUFFIX ".dylib")
target_link_libraries(kuzu_java_native ${JNI_LIBRARIES} ${KUZU})

# add test to run JNIFoo
add_test(NAME Test
        COMMAND ${Java_JAVA_EXECUTABLE}
        -ea
        -Djava.library.path=${CMAKE_CURRENT_BINARY_DIR}
        -cp ${_jarFile} tools.java_api.test)