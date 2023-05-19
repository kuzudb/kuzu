#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundAddProperty : public BoundDDL {
public:
    explicit BoundAddProperty(common::table_id_t tableID, std::string propertyName,
        common::LogicalType dataType, std::shared_ptr<Expression> defaultValue,
        std::string tableName)
        : BoundDDL{common::StatementType::ADD_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValue{std::move(defaultValue)} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::string getPropertyName() const { return propertyName; }

    inline common::LogicalType getDataType() const { return dataType; }

    inline std::shared_ptr<Expression> getDefaultValue() const { return defaultValue; }

private:
    common::table_id_t tableID;
    std::string propertyName;
    common::LogicalType dataType;
    std::shared_ptr<Expression> defaultValue;
};

} // namespace binder
} // namespace kuzu
