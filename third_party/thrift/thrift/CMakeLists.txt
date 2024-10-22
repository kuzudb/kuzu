add_library(thrift
        STATIC
        protocol/TProtocol.cpp
        transport/TTransportException.cpp
        transport/TBufferTransports.cpp)

target_include_directories(thrift PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
