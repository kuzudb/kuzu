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
add_jar(kuzu_java_base SOURCES ${JAVA_SRC_FILES} GENERATE_NATIVE_HEADERS kuzu_native_header)

add_library(kuzu_java_native SHARED src/jni/kuzu_java.cpp)
target_link_libraries(kuzu_java_native PRIVATE kuzu_native_header kuzu)

message(STATUS "OS_NAME=${OS_NAME}")
message(STATUS "OS_ARCH=${OS_ARCH}")
string(JOIN "_" LIB_SUFFIX ".so" ${OS_NAME} ${OS_ARCH})

set_target_properties(kuzu_java_native PROPERTIES SUFFIX ${LIB_SUFFIX})
set_target_properties(kuzu_java_native PROPERTIES PREFIX "lib")

# Copy kuzu_java_base.jar and add the shared library inside. This allows
# straightforward loading of the jar file, as opposed to distributing the
# shared library separately. We do a copy to make the modification times all
# work out for CMake, otherwise, updating the jar causes the native target to
# rebuild and be re-embedded, which causes the jar to update, and we never get
# a complete build.
add_custom_command(OUTPUT kuzu_java.jar
    COMMAND ${CMAKE_COMMAND} -E copy kuzu_java_base.jar kuzu_java.jar
    COMMAND ${Java_JAR_EXECUTABLE} uf kuzu_java.jar -C
            $<TARGET_FILE_DIR:kuzu_java_native> $<TARGET_FILE_NAME:kuzu_java_native>
    DEPENDS kuzu_java_native kuzu_java_base)
add_custom_target(kuzu_java ALL DEPENDS kuzu_java.jar)
install(FILES kuzu_java.jar DESTINATION ${CMAKE_INSTALL_LIBDIR}/java)

file(GLOB JAVA_TEST_FILES src/test/java/com/kuzudb/test/*.java)
add_jar(kuzu_java_test ${JAVA_TEST_FILES}
    INCLUDE_JARS ${CMAKE_CURRENT_BINARY_DIR}/kuzu_java.jar third_party/junit-platform-console-standalone-1.9.3.jar)
