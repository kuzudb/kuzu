#include "processor/warning_context.h"

#include "common/uniq_lock.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

WarningContext::WarningContext(main::ClientConfig* clientConfig) : clientConfig(clientConfig) {}

void WarningContext::appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
    uint64_t queryID) {
    common::UniqLock lock{mtx};

    for (const auto& message : messages) {
        if (warnings.size() >= clientConfig->warningLimit) {
            return;
        }
        warnings.emplace_back(message, queryID);
    }
}

} // namespace processor
} // namespace kuzu
