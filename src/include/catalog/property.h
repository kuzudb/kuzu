#pragma once

#include "common/copy_constructors.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common

namespace catalog {

class Property {
public:
    Property() = default;
    Property(std::string name, common::LogicalType dataType,
        std::unique_ptr<parser::ParsedExpression> defaultExpr)
        : Property{std::move(name), std::move(dataType), std::move(defaultExpr),
              common::INVALID_PROPERTY_ID, common::INVALID_COLUMN_ID, common::INVALID_TABLE_ID} {}
    Property(std::string name, common::LogicalType dataType,
        std::unique_ptr<parser::ParsedExpression> defaultExpr, common::property_id_t propertyID,
        common::column_id_t columnID, common::table_id_t tableID)
        : name{std::move(name)}, defaultExpr{std::move(defaultExpr)}, dataType{std::move(dataType)},
          propertyID{propertyID}, columnID{columnID}, tableID{tableID} {}
    EXPLICIT_COPY_DEFAULT_MOVE(Property);

    std::string getName() const { return name; }

    const common::LogicalType& getDataType() const { return dataType; }
    common::property_id_t getPropertyID() const { return propertyID; }
    common::column_id_t getColumnID() const { return columnID; }
    common::table_id_t getTableID() const { return tableID; }
    const parser::ParsedExpression* getDefaultExpr() const { return defaultExpr.get(); }

    void rename(std::string newName) { name = std::move(newName); }

    void serialize(common::Serializer& serializer) const;
    static Property deserialize(common::Deserializer& deserializer);

    static std::string toCypher(const std::vector<kuzu::catalog::Property>& properties);

private:
    Property(const Property& other)
        : name{other.name}, defaultExpr{other.defaultExpr->copy()}, dataType{other.dataType.copy()},
          propertyID{other.propertyID}, columnID{other.columnID}, tableID{other.tableID} {}

private:
    std::string name;
    std::unique_ptr<parser::ParsedExpression> defaultExpr;
    common::LogicalType dataType;
    common::property_id_t propertyID;
    common::column_id_t columnID;
    common::table_id_t tableID;
};

} // namespace catalog
} // namespace kuzu
