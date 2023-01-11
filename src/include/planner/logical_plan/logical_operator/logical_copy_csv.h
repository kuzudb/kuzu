#pragma once

#include "base_logical_operator.h"
#include "catalog/catalog_structs.h"
#include "common/csv_reader/csv_reader.h"

namespace kuzu {
namespace planner {

using namespace kuzu::catalog;

class LogicalCopyCSV : public LogicalOperator {

public:
    LogicalCopyCSV(CSVDescription csvDescription, table_id_t tableID, string tableName)
        : LogicalOperator{LogicalOperatorType::COPY_CSV},
          csvDescription{std::move(csvDescription)}, tableID{tableID}, tableName{tableName} {}

    inline void computeSchema() override { createEmptySchema(); }

    inline string getExpressionsForPrinting() const override { return tableName; }

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline table_id_t getTableID() const { return tableID; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyCSV>(csvDescription, tableID, tableName);
    }

private:
    CSVDescription csvDescription;
    table_id_t tableID;
    // Used for printing only.
    string tableName;
};

} // namespace planner
} // namespace kuzu
