
if(MSVC)
else()
add_compile_options(-fPIC)
endif()

#-----------------------------------------------------------------------------------------------------------------------
include(FetchContent)
include(CheckCXXCompilerFlag)
include(CMakePrintHelpers)
include(CTest)

# CMAKE_SOURCE_DIR: ----------------------------------------------------------------------------------------------------
add_compile_definitions(CMAKE_SOURCE_DIR="${CMAKE_SOURCE_DIR}")

# Include --------------------------------------------------------------------------------------------------------------
include_directories(include)

# Src: -------------------------------------------------------------------------------------------------------
add_subdirectory(src)
