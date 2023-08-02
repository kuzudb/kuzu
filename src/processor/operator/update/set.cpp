#include "processor/operator/update/set.h"

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
        executor->set();
    }
    return true;
}

std::unique_ptr<PhysicalOperator> SetNodeProperty::clone() {
    std::vector<std::unique_ptr<NodeSetExecutor>> executorsCopy;
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return make_unique<SetNodeProperty>(
        std::move(executorsCopy), children[0]->clone(), id, paramsString);
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
        executor->set();
    }
    return true;
}

std::unique_ptr<PhysicalOperator> SetRelProperty::clone() {
    std::vector<std::unique_ptr<RelSetExecutor>> executorsCopy;
    for (auto& executor : executors) {
        executorsCopy.push_back(executor->copy());
    }
    return make_unique<SetRelProperty>(
        std::move(executorsCopy), children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
