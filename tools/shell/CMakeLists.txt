include_directories(include ${PROJECT_SOURCE_DIR}/third_party/taywee_args/include)
add_executable(kuzu_shell
        embedded_shell.cpp
        linenoise.cpp
        shell_runner.cpp)
if (NOT MSVC)
        set_target_properties(kuzu_shell PROPERTIES OUTPUT_NAME kuzu)
endif()

target_link_libraries(kuzu_shell kuzu)
if (MSVC)
        target_link_libraries(kuzu_shell ws2_32)
endif()

install(TARGETS kuzu_shell)
