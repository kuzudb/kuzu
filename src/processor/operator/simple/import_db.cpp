#include "processor/operator/simple/import_db.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

using std::stringstream;

void ImportDB::executeInternal(ExecutionContext* context) {
    context->clientContext->runQuery(query);
}

std::string ImportDB::getOutputMsg() {
    return "Imported database successfully.";
}

} // namespace processor
} // namespace kuzu
