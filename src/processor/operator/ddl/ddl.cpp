#include "processor/operator/ddl/ddl.h"

#include <string>

#include "processor/execution_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

void DDL::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outputVector = resultSet->getValueVector(outputPos).get();
}

bool DDL::getNextTuplesInternal(ExecutionContext* /*context*/) {
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
