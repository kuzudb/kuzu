#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalRenameProperty : public LogicalDDL {
public:
    explicit LogicalRenameProperty(table_id_t tableID, string tableName, property_id_t propertyID,
        string newName, shared_ptr<Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::RENAME_PROPERTY, std::move(tableName), outputExpression},
          tableID{tableID}, propertyID{propertyID}, newName{std::move(newName)} {}

    inline table_id_t getTableID() const { return tableID; }

    inline property_id_t getPropertyID() const { return propertyID; }

    inline string getNewName() const { return newName; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalRenameProperty>(
            tableID, tableName, propertyID, newName, outputExpression);
    }

private:
    table_id_t tableID;
    property_id_t propertyID;
    string newName;
};

} // namespace planner
} // namespace kuzu
