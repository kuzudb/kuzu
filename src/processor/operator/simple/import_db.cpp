#include "processor/operator/simple/import_db.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

using std::stringstream;

void ImportDB::executeInternal(ExecutionContext* context) {
    if (query.empty()) { // Export empty database.
        return;
    }
    // TODO(Guodong): this is special for "Import database". Should refactor after we support
    // multiple DDL and COPY statements in a single transaction.
    // Currently, we split multiple query statements into single query and execute them one by one,
    // each with an auto transaction.
    if (context->clientContext->getTransactionContext()->hasActiveTransaction()) {
        context->clientContext->getTransactionContext()->commit();
    }
    context->clientContext->queryInternal(query, "" /* encodedJoin */,
        false /* enumerateAllPlans */, std::nullopt /* queryID */);
}

std::string ImportDB::getOutputMsg() {
    return "Imported database successfully.";
}

} // namespace processor
} // namespace kuzu
