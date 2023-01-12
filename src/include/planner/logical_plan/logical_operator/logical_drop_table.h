#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDropTable : public LogicalDDL {
public:
    explicit LogicalDropTable(
        table_id_t tableID, string tableName, shared_ptr<Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::DROP_TABLE, std::move(tableName),
              std::move(outputExpression)},
          tableID{tableID} {}

    inline table_id_t getTableID() const { return tableID; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropTable>(tableID, tableName, outputExpression);
    }

private:
    table_id_t tableID;
};

} // namespace planner
} // namespace kuzu
