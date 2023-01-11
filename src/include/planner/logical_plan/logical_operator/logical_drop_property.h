#pragma once

#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalDropProperty : public LogicalDDL {

public:
    explicit LogicalDropProperty(table_id_t tableID, property_id_t propertyID, string tableName)
        : LogicalDDL{LogicalOperatorType::DROP_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyID{propertyID} {}

    inline table_id_t getTableID() const { return tableID; }

    inline property_id_t getPropertyID() const { return propertyID; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDropProperty>(tableID, propertyID, tableName);
    }

private:
    table_id_t tableID;
    property_id_t propertyID;
};

} // namespace planner
} // namespace kuzu
