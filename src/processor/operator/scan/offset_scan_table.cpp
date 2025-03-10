#include "processor/operator/scan/offset_scan_table.h"

#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

void OffsetScanTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    IDVector = resultSet->getValueVector(info.nodeIDPos).get();
}

bool OffsetScanTable::getNextTuplesInternal(ExecutionContext* context) {
    while (true) {
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        auto transaction = context->clientContext->getTransaction();
        // TODO(Rui): Finish this to perform lookups into the rel table.
    }
}

} // namespace processor
} // namespace kuzu
