#pragma once

#include "base_logical_operator.h"
#include "catalog/catalog_structs.h"
#include "common/csv_reader/csv_reader.h"

namespace kuzu {
namespace planner {

using namespace kuzu::catalog;

class LogicalCopyCSV : public LogicalOperator {

public:
    LogicalCopyCSV(CSVDescription csvDescription, TableSchema tableSchema)
        : LogicalOperator{LogicalOperatorType::COPY_CSV}, csvDescription{std::move(csvDescription)},
          tableSchema{std::move(tableSchema)} {}

    inline void computeSchema() override { createEmptySchema(); }

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
} // namespace kuzu
