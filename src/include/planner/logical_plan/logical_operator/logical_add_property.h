#pragma once

#include "logical_ddl.h"

namespace kuzu {
namespace planner {

class LogicalAddProperty : public LogicalDDL {
public:
    explicit LogicalAddProperty(table_id_t tableID, string propertyName, DataType dataType,
        shared_ptr<Expression> defaultValue, string tableName,
        shared_ptr<Expression> outputExpression)
        : LogicalDDL{LogicalOperatorType::ADD_PROPERTY, std::move(tableName), outputExpression},
          tableID{tableID}, propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValue{defaultValue} {}

    inline table_id_t getTableID() const { return tableID; }

    inline string getPropertyName() const { return propertyName; }

    inline DataType getDataType() const { return dataType; }

    inline shared_ptr<Expression> getDefaultValue() const { return defaultValue; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAddProperty>(
            tableID, propertyName, dataType, defaultValue, tableName, outputExpression);
    }

private:
    table_id_t tableID;
    string propertyName;
    DataType dataType;
    shared_ptr<Expression> defaultValue;
};

} // namespace planner
} // namespace kuzu
