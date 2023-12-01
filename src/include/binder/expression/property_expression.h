#pragma once

#include "common/constants.h"
#include "common/keyword/rdf_keyword.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(common::LogicalType dataType, const std::string& propertyName,
        const std::string& uniqueVariableName, const std::string& rawVariableName,
        std::unordered_map<common::table_id_t, common::property_id_t> propertyIDPerTable,
        bool isPrimaryKey_)
        : Expression{common::ExpressionType::PROPERTY, std::move(dataType),
              uniqueVariableName + "." + propertyName},
          isPrimaryKey_{isPrimaryKey_}, propertyName{propertyName},
          uniqueVariableName{uniqueVariableName}, rawVariableName{rawVariableName},
          propertyIDPerTable{std::move(propertyIDPerTable)} {}

    PropertyExpression(const PropertyExpression& other)
        : Expression{common::ExpressionType::PROPERTY, other.dataType, other.uniqueName},
          isPrimaryKey_{other.isPrimaryKey_}, propertyName{other.propertyName},
          uniqueVariableName{other.uniqueVariableName}, rawVariableName{other.rawVariableName},
          propertyIDPerTable{other.propertyIDPerTable} {}

    inline bool isPrimaryKey() const { return isPrimaryKey_; }

    inline std::string getPropertyName() const { return propertyName; }

    inline std::string getVariableName() const { return uniqueVariableName; }

    inline bool hasPropertyID(common::table_id_t tableID) const {
        return propertyIDPerTable.contains(tableID);
    }
    inline common::property_id_t getPropertyID(common::table_id_t tableID) const {
        KU_ASSERT(propertyIDPerTable.contains(tableID));
        return propertyIDPerTable.at(tableID);
    }

    inline bool isInternalID() const { return getPropertyName() == common::InternalKeyword::ID; }
    inline bool isIRI() const { return getPropertyName() == common::rdf::IRI; }

    inline std::unique_ptr<Expression> copy() const override {
        return make_unique<PropertyExpression>(*this);
    }

    inline std::string toStringInternal() const final {
        return rawVariableName + "." + propertyName;
    }

private:
    bool isPrimaryKey_ = false;
    std::string propertyName;
    // unique identifier references to a node/rel table.
    std::string uniqueVariableName;
    // printable identifier references to a node/rel table.
    std::string rawVariableName;
    std::unordered_map<common::table_id_t, common::property_id_t> propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
