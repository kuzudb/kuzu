#include "function/table/scan_functions.h"

namespace kuzu {
namespace function {

std::pair<uint64_t, uint64_t> ScanSharedState::getNext() {
    std::lock_guard<std::mutex> guard{lock};
    if (fileIdx >= readerConfig.getNumFiles()) {
        return {UINT64_MAX, UINT64_MAX};
    }
    return {fileIdx, blockIdx++};
}

} // namespace function
} // namespace kuzu
