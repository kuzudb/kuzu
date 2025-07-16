#include "processor/operator/aggregate/base_aggregate_scan.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

void BaseAggregateScan::initLocalStateInternal(ResultSet* resultSet,
    ExecutionContext* /*context*/) {
    for (auto& dataPos : scanInfo.aggregatesPos) {
        auto valueVector = resultSet->getValueVector(dataPos);
        aggregateVectors.push_back(valueVector);
    }
}

} // namespace processor
} // namespace kuzu
