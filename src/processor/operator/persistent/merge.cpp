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
    KU_ASSERT(existenceVector->state->getSelVector().getSelSize() == 1);
    auto pos = existenceVector->state->getSelVector()[0];
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
            KU_ASSERT(distinctVector->state->getSelVector().getSelSize() == 1);
            auto distinctPos = distinctVector->state->getSelVector()[0];
            patternHasBeenCreated = !distinctVector->getValue<bool>(distinctPos);
        }
        if (patternHasBeenCreated) {
            for (auto& executor : nodeInsertExecutors) {
                executor.skipInsert();
            }
            for (auto& executor : relInsertExecutors) {
                executor.skipInsert();
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
                executor.insert(context->clientContext->getTx());
            }
            for (auto& executor : relInsertExecutors) {
                executor.insert(context->clientContext->getTx());
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

std::unique_ptr<PhysicalOperator> Merge::clone() {
    return std::make_unique<Merge>(existenceMark, distinctMark, copyVector(nodeInsertExecutors),
        copyVector(relInsertExecutors), NodeSetExecutor::copy(onCreateNodeSetExecutors),
        RelSetExecutor::copy(onCreateRelSetExecutors),
        NodeSetExecutor::copy(onMatchNodeSetExecutors),
        RelSetExecutor::copy(onMatchRelSetExecutors), children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu
