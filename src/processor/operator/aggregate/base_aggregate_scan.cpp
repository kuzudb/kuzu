#include "processor/operator/aggregate/base_aggregate_scan.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

void BaseAggregateScan::initLocalStateInternal(
    ResultSet* resultSet, ExecutionContext* /*context*/) {
    for (auto& dataPos : aggregatesPos) {
        auto valueVector = resultSet->getValueVector(dataPos);
        aggregateVectors.push_back(valueVector);
    }
}

void BaseAggregateScan::writeAggregateResultToVector(
    ValueVector& vector, uint64_t pos, AggregateState* aggregateState) {
    if (aggregateState->isNull) {
        vector.setNull(pos, true);
    } else {
        aggregateState->moveResultToVector(&vector, pos);
    }
}

} // namespace processor
} // namespace kuzu
