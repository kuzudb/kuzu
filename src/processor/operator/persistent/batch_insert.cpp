#include "processor/operator/persistent/batch_insert.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

void BatchInsert::checkIfTableIsEmpty(Transaction* transaction) {
    if (sharedState->table->getNumTuples(transaction) != 0) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
}

} // namespace processor
} // namespace kuzu
