#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDropTable : public LogicalOperator {

public:
    explicit LogicalDropTable(TableSchema* tableSchema)
        : LogicalOperator{LogicalOperatorType::DROP_TABLE}, tableSchema{tableSchema} {}

    inline TableSchema* getTableSchema() const { return tableSchema; }

    void computeSchema() override { createEmptySchema(); }

    inline string getExpressionsForPrinting() const override { return tableSchema->tableName; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropTable>(tableSchema);
    }

private:
    TableSchema* tableSchema;
};

} // namespace planner
} // namespace kuzu
