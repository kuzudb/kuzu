#include "processor/operator/persistent/insert.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string InsertPrintInfo::toString() const {
    std::string result = "Expressions: ";
    result += binder::ExpressionUtil::toString(expressions);
    result += ", Action: ";
    result += ConflictActionUtil::toString(action);
    return result;
}

void Insert::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& executor : nodeExecutors) {
        executor.init(resultSet, context);
    }
    for (auto& executor : relExecutors) {
        executor.init(resultSet, context);
    }
}

bool Insert::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& executor : nodeExecutors) {
        executor.insert(context->clientContext->getTransaction());
    }
    for (auto& executor : relExecutors) {
        executor.insert(context->clientContext->getTransaction());
    }
    return true;
}

} // namespace processor
} // namespace kuzu
