#include "processor/operator/persistent/merge.h"

#include "binder/expression/expression_util.h"

namespace kuzu {
namespace processor {

std::string MergePrintInfo::toString() const {
    std::string result = "Pattern: ";
    result += binder::ExpressionUtil::toString(pattern);
    if (!onMatch.empty()) {
        result += ", ON MATCH SET: " + binder::ExpressionUtil::toString(onMatch);
    }
    if (!onCreate.empty()) {
        result += ", ON CREATE SET: " + binder::ExpressionUtil::toString(onCreate);
    }
    return result;
}

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
                KU_ASSERT(executor.getNodeIDVector()->state->getSelVector().getSelSize() == 1);
                // Note: The node ID is set to NULL after the left join. We need to set the node ID
                // vector to non-null before inserting.
                executor.getNodeIDVector()->setNull(
                    executor.getNodeIDVector()->state->getSelVector()[0], false);
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
        copyVector(relInsertExecutors), copyVector(onCreateNodeSetExecutors),
        copyVector(onCreateRelSetExecutors), copyVector(onMatchNodeSetExecutors),
        copyVector(onMatchRelSetExecutors), children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu
