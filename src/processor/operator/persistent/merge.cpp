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
    KU_ASSERT(existenceVector->state->selVector->selectedSize == 1);
    auto pos = existenceVector->state->selVector->selectedPositions[0];
    auto patternExist = existenceVector->getValue<bool>(pos);
    if (patternExist) {
        for (auto& executor : onMatchNodeSetExecutors) {
            executor->set(context);
        }
        for (auto& executor : onMatchRelSetExecutors) {
            executor->set(context);
        }
    } else {
        auto patternHasBeenCreated = false;
        if (distinctVector != nullptr) {
            KU_ASSERT(distinctVector->state->selVector->selectedSize == 1);
            auto pos = distinctVector->state->selVector->selectedPositions[0];
            patternHasBeenCreated = !distinctVector->getValue<bool>(pos);
        }
        if (patternHasBeenCreated) {
            for (auto& executor : nodeInsertExecutors) {
                executor.skipInsert(context);
            }
            for (auto& executor : relInsertExecutors) {
                executor.skipInsert(context);
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
