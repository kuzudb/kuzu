#include "processor/operator/persistent/import_db.h"

using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

using std::stringstream;

bool ImportDB::getNextTuplesInternal(ExecutionContext* context) {
    context->clientContext->runQuery(query);
    return false;
}
} // namespace processor
} // namespace kuzu
