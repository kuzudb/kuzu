#pragma once

#include <thread>

#include "nlohmann/json.hpp"

#include "src/common/include/configs.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace main {

class System;

class SystemConfig {
    friend class System;

public:
    bool setBufferPoolSize(const uint64_t& size);
    bool setNumProcessorThreads(const uint32_t& num);

    unique_ptr<nlohmann::json> debugInfo();

private:
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
    uint64_t numProcessorThreads = thread::hardware_concurrency();
};

} // namespace main
} // namespace graphflow
