#pragma once

#include "planner/logical_plan/ddl/logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalDropProperty : public LogicalDDL {
public:
    explicit LogicalDropProperty(common::table_id_t tableID, common::property_id_t propertyID,
        std::string tableName, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::DROP_PROPERTY, std::move(tableName),
              std::move(outputExpression)},
          tableID{tableID}, propertyID{propertyID} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline common::property_id_t getPropertyID() const { return propertyID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropProperty>(tableID, propertyID, tableName, outputExpression);
    }

private:
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace planner
} // namespace kuzu
