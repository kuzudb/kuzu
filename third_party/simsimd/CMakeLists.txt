if(NOT MSVC)
    add_compile_options(-Wno-extra)
endif()

add_library(simsimd
        STATIC
        lib.c)

target_include_directories(simsimd PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR})
