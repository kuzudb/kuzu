#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> FactorizedTableScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    for (auto& pos : flatDataChunkPositions) {
        resultSet->dataChunks[pos]->state = DataChunkState::getSingleValueDataChunkState();
    }
    initFurther(context);
    return resultSet;
}

} // namespace processor
} // namespace kuzu
