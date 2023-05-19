#pragma once

#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalAddProperty : public LogicalDDL {
public:
    explicit LogicalAddProperty(common::table_id_t tableID, std::string propertyName,
        common::LogicalType dataType, std::shared_ptr<binder::Expression> defaultValue,
        std::string tableName, std::shared_ptr<binder::Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::ADD_PROPERTY, std::move(tableName),
              std::move(outputExpression)},
          tableID{tableID}, propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValue{std::move(defaultValue)} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::string getPropertyName() const { return propertyName; }

    inline common::LogicalType getDataType() const { return dataType; }

    inline std::shared_ptr<binder::Expression> getDefaultValue() const { return defaultValue; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAddProperty>(
            tableID, propertyName, dataType, defaultValue, tableName, outputExpression);
    }

private:
    common::table_id_t tableID;
    std::string propertyName;
    common::LogicalType dataType;
    std::shared_ptr<binder::Expression> defaultValue;
};

} // namespace planner
} // namespace kuzu
