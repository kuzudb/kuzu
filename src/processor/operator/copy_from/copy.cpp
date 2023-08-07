#include "processor/operator/copy_from/copy.h"

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string Copy::execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    registerProfilingMetrics(executionContext->profiler);
    metrics->executionTime.start();
    if (!isCopyAllowed()) {
        throw CopyException("COPY commands can only be executed once on a table.");
    }
    auto numTuplesCopied = executeInternal(taskScheduler, executionContext);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(numTuplesCopied);
    return getOutputMsg(numTuplesCopied);
}

std::string Copy::getOutputMsg(uint64_t numTuplesCopied) {
    return StringUtils::string_format("{} number of tuples has been copied to table: {}.",
        numTuplesCopied, catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
}

} // namespace processor
} // namespace kuzu
