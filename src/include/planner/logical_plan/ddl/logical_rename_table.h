#pragma once

#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalRenameTable : public LogicalDDL {
public:
    explicit LogicalRenameTable(common::table_id_t tableID, std::string tableName,
        std::string newName, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::RENAME_TABLE, std::move(tableName),
              std::move(outputExpression)},
          tableID{tableID}, newName{std::move(newName)} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::string getNewName() const { return newName; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalRenameTable>(tableID, tableName, newName, outputExpression);
    }

private:
    common::table_id_t tableID;
    std::string newName;
};

} // namespace planner
} // namespace kuzu
