#pragma once

#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalRenameProperty : public LogicalDDL {
public:
    explicit LogicalRenameProperty(common::table_id_t tableID, std::string tableName,
        common::property_id_t propertyID, std::string newName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::RENAME_PROPERTY, std::move(tableName),
              std::move(outputExpression)},
          tableID{tableID}, propertyID{propertyID}, newName{std::move(newName)} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline common::property_id_t getPropertyID() const { return propertyID; }

    inline std::string getNewName() const { return newName; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalRenameProperty>(
            tableID, tableName, propertyID, newName, outputExpression);
    }

private:
    common::table_id_t tableID;
    common::property_id_t propertyID;
    std::string newName;
};

} // namespace planner
} // namespace kuzu
