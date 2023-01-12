#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

void DDL::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    outputVector = resultSet->getValueVector(outputPos).get();
}

bool DDL::getNextTuplesInternal() {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    executeDDLInternal();
    outputVector->setValue<std::string>(0, getOutputMsg());
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
