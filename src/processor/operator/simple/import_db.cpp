#include "processor/operator/simple/import_db.h"

#include "common/exception/runtime.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

static void validateQueryResult(main::QueryResult* queryResult) {
    auto currentResult = queryResult;
    while (currentResult) {
        if (!currentResult->isSuccess()) {
            throw RuntimeException("Import database failed: " + currentResult->getErrorMessage());
        }
        currentResult = currentResult->getNextQueryResult();
    }
}

void ImportDB::executeInternal(ExecutionContext* context) {
    auto clientContext = context->clientContext;
    if (query.empty()) { // Export empty database.
        appendMessage("Imported database successfully.", clientContext->getMemoryManager());
        return;
    }
    // TODO(Guodong): this is special for "Import database". Should refactor after we support
    // multiple DDL and COPY statements in a single transaction.
    // Currently, we split multiple query statements into single query and execute them one by one,
    // each with an auto transaction.
    auto transactionContext = clientContext->getTransactionContext();
    if (transactionContext->hasActiveTransaction()) {
        transactionContext->commit();
    }
    auto res = clientContext->queryNoLock(query);
    validateQueryResult(res.get());
    if (!indexQuery.empty()) {
        res = clientContext->queryNoLock(indexQuery);
        validateQueryResult(res.get());
    }
    appendMessage("Imported database successfully.", clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu
