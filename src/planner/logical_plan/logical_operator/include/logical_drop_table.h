#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDropTable : public LogicalOperator {

public:
    explicit LogicalDropTable(TableSchema* tableSchema)
        : LogicalOperator{}, tableSchema{tableSchema} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_DROP_TABLE;
    }

    inline TableSchema* getTableSchema() const { return tableSchema; }

    inline string getExpressionsForPrinting() const override { return tableSchema->tableName; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropTable>(tableSchema);
    }

private:
    TableSchema* tableSchema;
};

} // namespace planner
} // namespace kuzu
