#include "src/main/include/system_config.h"

namespace graphflow {
namespace main {

bool SystemConfig::setBufferPoolSize(const uint64_t& size) {
    if (size < (1 << 20) || size > (1ul << 40u)) {
        throw invalid_argument(
            "Buffer pool size argument should be graeter than 1MB or less than 1TB.");
    }
    if (0 != size % PAGE_SIZE) {
        throw invalid_argument(
            "Buffer pool size should be aligned to the system PAGE SIZE (=4096B)");
    }
    bufferPoolSize = size;
    return true;
}

bool SystemConfig::setNumProcessorThreads(const uint32_t& num) {
    if (num < 1 || num > thread::hardware_concurrency()) {
        throw invalid_argument(
            "Number should be greater than 0 and less than the number of physical threads "
            "available in the system.");
    }
    if (numProcessorThreads == num) {
        throw invalid_argument("No change in num.");
    }
    numProcessorThreads = num;
    return true;
}

unique_ptr<nlohmann::json> SystemConfig::debugInfo() {
    auto json = make_unique<nlohmann::json>();
    (*json)["bufferPoolSize"] = bufferPoolSize;
    (*json)["numProcessorThreads"] = numProcessorThreads;
    return json;
}

} // namespace main
} // namespace graphflow
