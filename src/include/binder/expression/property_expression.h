#pragma once

#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "expression.h"

namespace kuzu {
namespace binder {

struct SingleLabelPropertyInfo {
    bool isPrimaryKey;
    common::property_id_t id;

    SingleLabelPropertyInfo(bool isPrimaryKey, common::property_id_t id)
        : isPrimaryKey{isPrimaryKey}, id{id} {}
    EXPLICIT_COPY_DEFAULT_MOVE(SingleLabelPropertyInfo);

private:
    SingleLabelPropertyInfo(const SingleLabelPropertyInfo& other)
        : isPrimaryKey{other.isPrimaryKey}, id{other.id} {}
};

class PropertyExpression : public Expression {
public:
    PropertyExpression(common::LogicalType dataType, const std::string& propertyName,
        const std::string& uniqueVariableName, const std::string& rawVariableName,
        common::table_id_map_t<SingleLabelPropertyInfo> infos)
        : Expression{common::ExpressionType::PROPERTY, std::move(dataType),
              uniqueVariableName + "." + propertyName},
          propertyName{propertyName}, uniqueVariableName{uniqueVariableName},
          rawVariableName{rawVariableName}, infos{std::move(infos)} {}

    PropertyExpression(const PropertyExpression& other)
        : Expression{common::ExpressionType::PROPERTY, other.dataType, other.uniqueName},
          propertyName{other.propertyName}, uniqueVariableName{other.uniqueVariableName},
          rawVariableName{other.rawVariableName}, infos{copyMap(other.infos)} {}

    // If this property is primary key on all tables.
    bool isPrimaryKey() const;
    // If this property is primary key for given table.
    bool isPrimaryKey(common::table_id_t tableID) const;

    std::string getPropertyName() const { return propertyName; }

    std::string getVariableName() const { return uniqueVariableName; }

    // If this property exists for given table.
    bool hasPropertyID(common::table_id_t tableID) const;
    common::property_id_t getPropertyID(common::table_id_t tableID) const;

    bool isInternalID() const { return getPropertyName() == common::InternalKeyword::ID; }
    bool isIRI() const { return getPropertyName() == common::rdf::IRI; }

    std::unique_ptr<Expression> copy() const override {
        return make_unique<PropertyExpression>(*this);
    }

    std::string toStringInternal() const final { return rawVariableName + "." + propertyName; }

private:
    std::string propertyName;
    // unique identifier references to a node/rel table.
    std::string uniqueVariableName;
    // printable identifier references to a node/rel table.
    std::string rawVariableName;
    // The same property name may have different info on each table.
    common::table_id_map_t<SingleLabelPropertyInfo> infos;
};

} // namespace binder
} // namespace kuzu
