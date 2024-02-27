#include "processor/operator/persistent/batch_insert.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void BatchInsert::checkIfTableIsEmpty() {
    if (sharedState->table->getNumTuples() != 0) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
}

} // namespace processor
} // namespace kuzu
