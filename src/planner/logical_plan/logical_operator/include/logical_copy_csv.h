#pragma once

#include "base_logical_operator.h"

#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/csv_reader/csv_reader.h"

namespace graphflow {
namespace planner {

using namespace graphflow::catalog;

class LogicalCopyCSV : public LogicalOperator {

public:
    LogicalCopyCSV(CSVDescription csvDescription, TableSchema tableSchema)
        : LogicalOperator{}, csvDescription{move(csvDescription)}, tableSchema{move(tableSchema)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_COPY_CSV; }

    inline string getExpressionsForPrinting() const override { return tableSchema.tableName; }

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline TableSchema getTableSchema() const { return tableSchema; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyCSV>(csvDescription, tableSchema);
    }

private:
    CSVDescription csvDescription;
    TableSchema tableSchema;
};

} // namespace planner
} // namespace graphflow
