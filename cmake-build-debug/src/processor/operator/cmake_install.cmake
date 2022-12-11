# Install script for directory: /Users/calvin/Desktop/kuzu_cmake/src/processor/operator

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/Library/Developer/CommandLineTools/usr/bin/objdump")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/aggregate/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/copy_csv/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/ddl/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/hash_join/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/intersect/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/order_by/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/scan_column/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/scan_list/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/table_scan/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/update/cmake_install.cmake")
  include("/Users/calvin/Desktop/kuzu_cmake/cmake-build-debug/src/processor/operator/var_length_extend/cmake_install.cmake")

endif()

