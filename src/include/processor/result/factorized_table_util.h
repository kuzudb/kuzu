#pragma once

#include "factorized_table.h"
#include "planner/operator/schema.h"

namespace kuzu {
namespace processor {

class FactorizedTableUtils {
public:
    static FactorizedTableSchema createFTableSchema(const binder::expression_vector& exprs,
        const planner::Schema& schema);
    static FactorizedTableSchema createFlatTableSchema(
        std::vector<common::LogicalType> columnTypes);

    // TODO(Ziyi): These two functions are used to store the copy message in a factorizedTable
    // because the current QueryProcessor::execute requires the last operator in the physical plan
    // must be ResultCollector. We should remove this class after we remove the assumption that the
    // last operator in the pipeline must be resultCollector.
    static void appendStringToTable(FactorizedTable* factorizedTable, std::string& outputMsg,
        storage::MemoryManager* memoryManager);
    static std::shared_ptr<FactorizedTable> getFactorizedTableForOutputMsg(std::string& outputMsg,
        storage::MemoryManager* memoryManager);
    static std::shared_ptr<FactorizedTable> getSingleStringColumnFTable(storage::MemoryManager* mm);
};

} // namespace processor
} // namespace kuzu
