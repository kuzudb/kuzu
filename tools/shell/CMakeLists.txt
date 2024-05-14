include_directories(include ${PROJECT_SOURCE_DIR}/third_party/taywee_args/include)

add_executable(kuzu_shell
        embedded_shell.cpp
        linenoise.cpp
        shell_runner.cpp)

target_link_libraries(kuzu_shell kuzu)
if (ENABLE_BACKTRACES)
        target_link_libraries(kuzu_shell register_backtrace_signal_handler)
endif()
if (MSVC)
        target_link_libraries(kuzu_shell ws2_32)
endif()

install(TARGETS kuzu_shell)
