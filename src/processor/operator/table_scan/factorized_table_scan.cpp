#include "include/factorized_table_scan.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> FactorizedTableScan::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    for (auto& pos : flatDataChunkPositions) {
        resultSet->dataChunks[pos]->state = DataChunkState::getSingleValueDataChunkState();
    }
    initFurther(context);
    sharedState->setToInitialState();
    return resultSet;
}

} // namespace processor
} // namespace graphflow
