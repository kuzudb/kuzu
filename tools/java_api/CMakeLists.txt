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

file(GLOB JAVA_TEST_FILES src/test/java/com/kuzudb/test/*.java)
add_jar(kuzu_java_test ${JAVA_TEST_FILES}
    INCLUDE_JARS ${CMAKE_CURRENT_BINARY_DIR}/kuzu_java_base.jar third_party/junit-platform-console-standalone-1.9.3.jar)

set(JAVA_DEST ${CMAKE_INSTALL_LIBDIR}/java)
install(TARGETS kuzu_java_native DESTINATION ${JAVA_DEST})
install_jar(kuzu_java_base DESTINATION ${JAVA_DEST})
# Custom Java installation logic.
# Pass needed variables to main commands.
install(CODE "set(JAVA_DEST \"${JAVA_DEST}\")")
install(CODE "set(Java_JAR_EXECUTABLE \"${Java_JAR_EXECUTABLE}\")")
# Generator expressions are expanded in `[[`, but not variables.
install(CODE [[
    file(RENAME ${CMAKE_INSTALL_PREFIX}/${JAVA_DEST}/kuzu_java_base.jar ${CMAKE_INSTALL_PREFIX}/${JAVA_DEST}/kuzu_java.jar)
    execute_process(COMMAND
        ${Java_JAR_EXECUTABLE} uf
        ${CMAKE_INSTALL_PREFIX}/${JAVA_DEST}/kuzu_java.jar
        -C ${CMAKE_INSTALL_PREFIX}/${JAVA_DEST} $<TARGET_FILE_NAME:kuzu_java_native>
        COMMAND_ERROR_IS_FATAL ANY
    )
]])
