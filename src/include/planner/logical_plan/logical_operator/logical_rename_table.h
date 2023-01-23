#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalRenameTable : public LogicalDDL {
public:
    explicit LogicalRenameTable(table_id_t tableID, string tableName, string newName,
        shared_ptr<Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::RENAME_TABLE, std::move(tableName), outputExpression},
          tableID{tableID}, newName{std::move(newName)} {}

    inline table_id_t getTableID() const { return tableID; }

    inline string getNewName() const { return newName; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalRenameTable>(tableID, tableName, newName, outputExpression);
    }

private:
    table_id_t tableID;
    string newName;
};

} // namespace planner
} // namespace kuzu
