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
else()
    message (FATAL_ERROR "Java/JNI not found")
endif()
include(UseJava)

file(GLOB JAVA_SRC_FILES src/main/java/com/kuzudb/*.java)

set(CMAKE_JAVA_COMPILE_FLAGS -source 1.8 -target 1.8 -encoding utf-8)
add_jar(kuzu_java ${JAVA_SRC_FILES} 
    OUTPUT_DIR "${PROJECT_SOURCE_DIR}/build" 
    GENERATE_NATIVE_HEADERS kuzu_native_header)
get_target_property(_jarFile kuzu_java JAR_FILE)
get_target_property(_classDir kuzu_java CLASSDIR)

set (_stubDir "${CMAKE_CURRENT_BINARY_DIR}")

include_directories(${JNI_INCLUDE_DIRS} ${_classDir} ${_stubDir})
add_library(kuzu_java_native SHARED src/jni/kuzu_java.cpp)
target_link_libraries(kuzu_java_native PRIVATE kuzu_native_header kuzu)

message(STATUS "OS_NAME=${OS_NAME}")
message(STATUS "OS_ARCH=${OS_ARCH}")
string(JOIN "_" LIB_SUFFIX ".so" ${OS_NAME} ${OS_ARCH})

set_target_properties(kuzu_java_native PROPERTIES SUFFIX ${LIB_SUFFIX})
set_target_properties(kuzu_java_native PROPERTIES PREFIX "lib")
set_target_properties(kuzu_java_native
        PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build"
        RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build"
        ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/build")

add_custom_command(OUTPUT dummy_jar_target
    DEPENDS kuzu_java_native kuzu_java
    COMMAND ${Java_JAR_EXECUTABLE} uf ${_jarFile} -C 
            $<TARGET_FILE_DIR:kuzu_java_native> $<TARGET_FILE_NAME:kuzu_java_native>)

add_custom_target(kuzu_java_api ALL DEPENDS dummy_jar_target)
