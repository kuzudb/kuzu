#include "processor/operator/copy/copy.h"

namespace kuzu {
namespace processor {

string Copy::execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) {
    registerProfilingMetrics(executionContext->profiler);
    metrics->executionTime.start();
    errorIfTableIsNonEmpty();
    auto numTuplesCopied = executeInternal(taskScheduler, executionContext);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(numTuplesCopied);
    return getOutputMsg(numTuplesCopied);
}

void Copy::errorIfTableIsNonEmpty() {
    auto numTuples = getNumTuplesInTable();
    if (numTuples > 0) {
        auto tableName = catalog->getReadOnlyVersion()->getTableSchema(tableID)->tableName;
        throw CopyException(
            "COPY commands can be executed only on completely empty tables. Table: " + tableName +
            " has " + to_string(numTuples) + " many tuples.");
    }
}

std::string Copy::getOutputMsg(uint64_t numTuplesCopied) {
    return StringUtils::string_format("%d number of tuples has been copied to table: %s.",
        numTuplesCopied, catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
}

} // namespace processor
} // namespace kuzu
