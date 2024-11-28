if (NOT TARGET test_helper)
        add_library(
                test_helper
                OBJECT
                test_helper.cpp
        )

        target_include_directories(
                test_helper
                PUBLIC
                ../include/
        )

        target_link_libraries(test_helper PUBLIC kuzu)
endif()

add_library(
        extension_test_helper
        OBJECT
        test_helper.cpp
)

target_include_directories(
        extension_test_helper
        PUBLIC
        ../include/
)

target_link_libraries(extension_test_helper PUBLIC kuzu)

if (NOT TARGET api_test_helper)
        add_library(
                api_test_helper
                OBJECT
                test_helper.cpp
        )

        target_include_directories(
                api_test_helper
                PUBLIC
                ../include/
        )
endif()

target_link_libraries(api_test_helper PUBLIC kuzu_shared)
