#pragma once

#include "base_logical_operator.h"
#include "catalog/catalog_structs.h"
#include "common/csv_reader/csv_reader.h"

namespace kuzu {
namespace planner {

using namespace kuzu::catalog;

class LogicalCopy : public LogicalOperator {

public:
    LogicalCopy(CopyDescription copyDescription, table_id_t tableID, string tableName)
        : LogicalOperator{LogicalOperatorType::COPY_CSV},
          copyDescription{std::move(copyDescription)}, tableID{tableID}, tableName{tableName} {}

    inline void computeSchema() override { createEmptySchema(); }

    inline string getExpressionsForPrinting() const override { return tableName; }

    inline CopyDescription getCopyDescription() const { return copyDescription; }

    inline table_id_t getTableID() const { return tableID; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopy>(copyDescription, tableID, tableName);
    }

private:
    CopyDescription copyDescription;
    table_id_t tableID;
    // Used for printing only.
    string tableName;
};

} // namespace planner
} // namespace kuzu
