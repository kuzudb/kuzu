#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void WarningContext::appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
    uint64_t queryID) {
    common::UniqLock lock{mtx};

    for (const auto& message : messages) {
        warnings.emplace_back(message, queryID);
    }
}

} // namespace processor
} // namespace kuzu
