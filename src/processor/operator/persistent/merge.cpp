#include "processor/operator/persistent/merge.h"

namespace kuzu {
namespace processor {

void Merge::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* context) {
    existenceVector = resultSet->getValueVector(existenceMark).get();
    if (distinctMark.isValid()) {
        distinctVector = resultSet->getValueVector(distinctMark).get();
    }
    for (auto& executor : nodeInsertExecutors) {
        executor.init(resultSet, context);
    }
    for (auto& executor : relInsertExecutors) {
        executor.init(resultSet, context);
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
    KU_ASSERT(existenceVector->state->isFlat());
    auto existencePos = existenceVector->state->selVector->selectedPositions[0];
    if (!existenceVector->isNull(existencePos)) {
        for (auto& executor : onMatchNodeSetExecutors) {
            executor->set(context);
        }
        for (auto& executor : onMatchRelSetExecutors) {
            executor->set(context);
        }
    } else {
        // pattern not exist
        if (distinctVector != nullptr &&
            !distinctVector->getValue<bool>(
                distinctVector->state->selVector->selectedPositions[0])) {
            // pattern has been created
            for (auto& executor : nodeInsertExecutors) {
                executor.evaluateResult(context);
            }
            for (auto& executor : relInsertExecutors) {
                executor.insert(context->clientContext->getTx(), context);
            }
            for (auto& executor : onMatchNodeSetExecutors) {
                executor->set(context);
            }
            for (auto& executor : onMatchRelSetExecutors) {
                executor->set(context);
            }
        } else {
            // do insert and on create
            for (auto& executor : nodeInsertExecutors) {
                executor.insert(context->clientContext->getTx(), context);
            }
            for (auto& executor : relInsertExecutors) {
                executor.insert(context->clientContext->getTx(), context);
            }
            for (auto& executor : onCreateNodeSetExecutors) {
                executor->set(context);
            }
            for (auto& executor : onCreateRelSetExecutors) {
                executor->set(context);
            }
        }
    }
    return true;
}

} // namespace processor
} // namespace kuzu
