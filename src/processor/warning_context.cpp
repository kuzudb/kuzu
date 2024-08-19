#include "processor/warning_context.h"

#include "common/string_format.h"
#include "common/uniq_lock.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void WarningContext::appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
    uint64_t queryID) {
    common::UniqLock lock{mtx};

    for (const auto& message : messages) {
        if (warnings.size() >= *warningLimit) {
            return;
        }
        warnings.emplace_back(message, queryID);
        if (warnings.size() == *warningLimit) {
            warnings.emplace_back(
                PopulatedCSVError{
                    .message = stringFormat("Reached warning limit of {}, any further "
                                            "warnings will not be reported.",
                        warnings.size()),
                    .filePath = "",
                    .reconstructedLine = "",
                    .lineNumber = 0,
                },
                queryID);
        }
    }
}

} // namespace processor
} // namespace kuzu
