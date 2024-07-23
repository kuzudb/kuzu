#include "processor/operator/persistent/set.h"

#include "binder/expression/expression_util.h"

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
    return std::make_unique<SetNodeProperty>(copyVector(executors), children[0]->clone(), id,
        printInfo->copy());
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
    return std::make_unique<SetRelProperty>(copyVector(executors), children[0]->clone(), id,
        printInfo->copy());
}

std::string SetPropertyPrintInfo::toString() const {
    std::string result = "Properties: ";
    result += binder::ExpressionUtil::toString(expressions);
    return result;
}
} // namespace processor
} // namespace kuzu
