#pragma once

#include <cstdint>
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
    // If using progress bar
    bool enableProgressBar;
};

struct ClientConfigDefault {
    // 0 means timeout is disabled by default.
    static constexpr uint64_t TIMEOUT_IN_MS = 0;
    static constexpr uint32_t VAR_LENGTH_MAX_DEPTH = 30;
    static constexpr bool ENABLE_SEMI_MASK = true;
    static constexpr bool ENABLE_PROGRESS_BAR = true;
};

} // namespace main
} // namespace kuzu
