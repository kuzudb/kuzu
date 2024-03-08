#pragma once

#include <string>

namespace kuzu {
namespace main {

struct ClientConfig {
    // System home directory.
    std::string homeDirectory;
    // File search path.
    std::string fileSearchPath;
    // If using semi mask in join.
    bool enableSemiMask;
    // Number of threads for execution.
    uint64_t numThreads;
    // Timeout (milliseconds)
    uint64_t timeoutInMS;
    // variable length maximum depth
    uint32_t varLengthMaxDepth;
};

} // namespace main
} // namespace kuzu
