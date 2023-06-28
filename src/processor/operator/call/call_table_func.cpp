#include "processor/operator/call/call_table_func.h"

namespace kuzu {
namespace processor {

std::pair<common::offset_t, common::offset_t> CallTableFuncSharedState::getNextBatch() {
    std::lock_guard guard{mtx};
    auto numTuplesInBatch = std::min(common::DEFAULT_VECTOR_CAPACITY, maxOffset - offset);
    offset += numTuplesInBatch;
    return std::make_pair(offset - numTuplesInBatch, offset);
}

void CallTableFunc::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& outputPos : callTableFuncInfo->outputPoses) {
        outputVectors.push_back(resultSet->getValueVector(outputPos).get());
    }
}

bool CallTableFunc::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    if (sharedState->hasNext()) {
        auto morsel = sharedState->getNextBatch();
        callTableFuncInfo->tableFunc(morsel, callTableFuncInfo->bindData.get(), outputVectors);
        metrics->numOutputTuple.increase(morsel.second - morsel.first);
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
