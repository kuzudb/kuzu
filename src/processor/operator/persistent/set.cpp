#include "processor/operator/persistent/set.h"

namespace kuzu {
namespace processor {

void SetNodeProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : executors) {
        executor->init(resultSet, context);
    }
}

bool SetNodeProperty::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : executors) {
        executor->set(context);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> SetNodeProperty::clone() {
    return std::make_unique<SetNodeProperty>(NodeSetExecutor::copy(executors), children[0]->clone(),
        id, paramsString);
}

void SetRelProperty::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : executors) {
        executor->init(resultSet, context);
    }
}

bool SetRelProperty::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : executors) {
        executor->set(context);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> SetRelProperty::clone() {
    return std::make_unique<SetRelProperty>(RelSetExecutor::copy(executors), children[0]->clone(),
        id, paramsString);
}

} // namespace processor
} // namespace kuzu
