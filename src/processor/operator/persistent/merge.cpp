#include "processor/operator/persistent/merge.h"

namespace kuzu {
namespace processor {

void Merge::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* context) {
    markVector = resultSet->getValueVector(markPos).get();
    for (auto& executor : nodeInsertExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : relInsertExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onCreateNodeSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onCreateRelSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onMatchNodeSetExecutors) {
        executor->init(resultSet, context);
    }
    for (auto& executor : onMatchRelSetExecutors) {
        executor->init(resultSet, context);
    }
}

bool Merge::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    KU_ASSERT(markVector->state->isFlat());
    auto pos = markVector->state->selVector->selectedPositions[0];
    if (!markVector->isNull(pos)) {
        for (auto& executor : onMatchNodeSetExecutors) {
            executor->set(context);
        }
        for (auto& executor : onMatchRelSetExecutors) {
            executor->set(context);
        }
    } else {
        for (auto& executor : nodeInsertExecutors) {
            executor->insert(transaction);
        }
        for (auto& executor : relInsertExecutors) {
            executor->insert(transaction);
        }
        for (auto& executor : onCreateNodeSetExecutors) {
            executor->set(context);
        }
        for (auto& executor : onCreateRelSetExecutors) {
            executor->set(context);
        }
    }
    return true;
}

} // namespace processor
} // namespace kuzu
