#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundAddProperty : public BoundDDL {
public:
    explicit BoundAddProperty(table_id_t tableID, string propertyName, DataType dataType,
        shared_ptr<Expression> defaultValue, string tableName)
        : BoundDDL{StatementType::ADD_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyName{std::move(propertyName)}, dataType{std::move(dataType)}, defaultValue{
                                                                                    defaultValue} {}

    inline table_id_t getTableID() const { return tableID; }

    inline string getPropertyName() const { return propertyName; }

    inline DataType getDataType() const { return dataType; }

    inline shared_ptr<Expression> getDefaultValue() const { return defaultValue; }

private:
    table_id_t tableID;
    string propertyName;
    DataType dataType;
    shared_ptr<Expression> defaultValue;
};

} // namespace binder
} // namespace kuzu
