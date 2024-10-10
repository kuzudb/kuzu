#pragma once

#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "expression.h"

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
}
namespace binder {

struct SingleLabelPropertyInfo {
    bool exists;
    bool isPrimaryKey;

    explicit SingleLabelPropertyInfo(bool exists, bool isPrimaryKey)
        : exists{exists}, isPrimaryKey{isPrimaryKey} {}
    EXPLICIT_COPY_DEFAULT_MOVE(SingleLabelPropertyInfo);

private:
    SingleLabelPropertyInfo(const SingleLabelPropertyInfo& other)
        : exists{other.exists}, isPrimaryKey{other.isPrimaryKey} {}
};

class PropertyExpression : public Expression {
    static constexpr common::ExpressionType expressionType_ = common::ExpressionType::PROPERTY;

public:
    PropertyExpression(common::LogicalType dataType, const std::string& propertyName,
        const std::string& uniqueVarName, const std::string& rawVariableName,
        common::table_id_map_t<SingleLabelPropertyInfo> infos)
        : Expression{expressionType_, std::move(dataType), uniqueVarName + "." + propertyName},
          propertyName{propertyName}, uniqueVarName{uniqueVarName},
          rawVariableName{rawVariableName}, infos{std::move(infos)} {}

    PropertyExpression(const PropertyExpression& other)
        : Expression{expressionType_, other.dataType.copy(), other.uniqueName},
          propertyName{other.propertyName}, uniqueVarName{other.uniqueVarName},
          rawVariableName{other.rawVariableName}, infos{copyMap(other.infos)} {}

    // Construct from a virtual property, i.e. no propertyID available.
    static std::unique_ptr<PropertyExpression> construct(common::LogicalType type,
        const std::string& propertyName, const Expression& pattern);

    // If this property is primary key on all tables.
    bool isPrimaryKey() const;
    // If this property is primary key for given table.
    bool isPrimaryKey(common::table_id_t tableID) const;

    std::string getPropertyName() const { return propertyName; }
    std::string getVariableName() const { return uniqueVarName; }
    std::string getRawVariableName() const { return rawVariableName; }

    // If this property exists for given table.
    bool hasProperty(common::table_id_t tableID) const;

    common::column_id_t getColumnID(const catalog::TableCatalogEntry& entry) const;

    bool isInternalID() const { return getPropertyName() == common::InternalKeyword::ID; }
    bool isIRI() const { return getPropertyName() == common::rdf::IRI; }

    std::unique_ptr<Expression> copy() const override {
        return make_unique<PropertyExpression>(*this);
    }

    std::string toStringInternal() const final { return rawVariableName + "." + propertyName; }

private:
    std::string propertyName;
    // unique identifier references to a node/rel table.
    std::string uniqueVarName;
    // printable identifier references to a node/rel table.
    std::string rawVariableName;
    // The same property name may have different info on each table.
    common::table_id_map_t<SingleLabelPropertyInfo> infos;
};

} // namespace binder
} // namespace kuzu
