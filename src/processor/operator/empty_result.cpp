#include "processor/operator/empty_result.h"

namespace kuzu {
namespace processor {

bool EmptyResult::getNextTuplesInternal(ExecutionContext*) {
    return false;
}

} // namespace processor
} // namespace kuzu
