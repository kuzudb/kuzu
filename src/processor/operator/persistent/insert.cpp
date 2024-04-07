#include "processor/operator/persistent/insert.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

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
        executor.insert(context->clientContext->getTx(), context);
    }
    for (auto& executor : relExecutors) {
        executor.insert(context->clientContext->getTx(), context);
    }
    return true;
}

} // namespace processor
} // namespace kuzu
