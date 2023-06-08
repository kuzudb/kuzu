#include "processor/operator/recursive_extend/scan_frontier.h"

namespace kuzu {
namespace processor {

bool ScanFrontier::getNextTuplesInternal(ExecutionContext* context) {
    if (!hasExecuted) {
        hasExecuted = true;
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
