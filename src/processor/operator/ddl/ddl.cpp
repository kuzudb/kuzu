#include "processor/operator/ddl/ddl.h"

#include "common/metric.h"

namespace kuzu {
namespace processor {

void DDL::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outputVector = resultSet->getValueVector(outputPos).get();
}

bool DDL::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    executeDDLInternal(context);
    outputVector->setValue<std::string>(0, getOutputMsg());
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace kuzu
