cmake_minimum_required (VERSION 3.11)

set(CMAKE_FIND_PACKAGE_RESOLVE_SYMLINKS TRUE)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

project (HelloWorldJNI)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)
if (JNI_FOUND)
    message (STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message (STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()
include(UseJava)

enable_testing()

# compile JNIFoo.java to class file
set(CMAKE_JAVA_COMPILE_FLAGS -source 1.8 -target 1.8 -encoding utf-8)
add_jar(HelloWorldJNI HelloWorldJNI.java)
get_target_property(_jarFile HelloWorldJNI JAR_FILE)
get_target_property(_classDir HelloWorldJNI CLASSDIR)

# generate JNIFoo.h stub
set (_stubDir "${CMAKE_CURRENT_BINARY_DIR}")
add_custom_command(
        OUTPUT HelloWorldJNI.h
        COMMAND ${Java_JAVAC_EXECUTABLE} 
        -h .
        HelloWorldJNI.java
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

add_library(native MODULE HelloWorldJNI.cpp HelloWorldJNI.h)
set_target_properties(native PROPERTIES SUFFIX ".dylib")
target_link_libraries(native ${JNI_LIBRARIES} ${KUZU})

# add test to run JNIFoo
add_test(NAME Test
        COMMAND ${Java_JAVA_EXECUTABLE}
        -Djava.library.path=${CMAKE_CURRENT_BINARY_DIR}
        -cp ${_jarFile} tools.java_api.HelloWorldJNI)