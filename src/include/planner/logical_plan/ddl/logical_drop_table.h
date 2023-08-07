#pragma once

#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDropTable : public LogicalDDL {
public:
    explicit LogicalDropTable(common::table_id_t tableID, std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::DROP_TABLE, std::move(tableName),
              std::move(outputExpression)},
          tableID{tableID} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropTable>(tableID, tableName, outputExpression);
    }

private:
    common::table_id_t tableID;
};

} // namespace planner
} // namespace kuzu
