#include "processor/operator/call/in_query_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::pair<offset_t, offset_t> InQueryCallSharedState::getNextBatch() {
    std::lock_guard guard{mtx};
    auto numTuplesInBatch = std::min(DEFAULT_VECTOR_CAPACITY, maxOffset - offset);
    offset += numTuplesInBatch;
    return std::make_pair(offset - numTuplesInBatch, offset);
}

void InQueryCall::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& outputPos : inQueryCallInfo->outputPoses) {
        outputVectors.push_back(resultSet->getValueVector(outputPos).get());
    }
}

bool InQueryCall::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto morsel = sharedState->getNextBatch();
    if (morsel.second > morsel.first) {
        inQueryCallInfo->tableFunc(morsel, inQueryCallInfo->bindData.get(), outputVectors);
        metrics->numOutputTuple.increase(morsel.second - morsel.first);
        return true;
    } else {
        return false;
    }
}

} // namespace processor
} // namespace kuzu
