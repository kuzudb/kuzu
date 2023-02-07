#pragma once

#include "common/configs.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(common::DataType dataType, const std::string& propertyName,
        const Expression& nodeOrRel,
        std::unordered_map<common::table_id_t, common::property_id_t> propertyIDPerTable,
        bool isPrimaryKey_)
        : Expression{common::PROPERTY, std::move(dataType),
              nodeOrRel.getUniqueName() + "." + propertyName},
          isPrimaryKey_{isPrimaryKey_}, propertyName{propertyName},
          variableName{nodeOrRel.getUniqueName()}, propertyIDPerTable{
                                                       std::move(propertyIDPerTable)} {
        rawName = nodeOrRel.getRawName() + "." + propertyName;
    }
    PropertyExpression(const PropertyExpression& other)
        : Expression{common::PROPERTY, other.dataType, other.uniqueName},
          isPrimaryKey_{other.isPrimaryKey_}, propertyName{other.propertyName},
          variableName{other.variableName}, propertyIDPerTable{other.propertyIDPerTable} {
        rawName = other.rawName;
    }

    inline bool isPrimaryKey() const { return isPrimaryKey_; }

    inline std::string getPropertyName() const { return propertyName; }

    inline std::string getVariableName() const { return variableName; }

    inline bool hasPropertyID(common::table_id_t tableID) const {
        return propertyIDPerTable.contains(tableID);
    }
    inline common::property_id_t getPropertyID(common::table_id_t tableID) const {
        assert(propertyIDPerTable.contains(tableID));
        return propertyIDPerTable.at(tableID);
    }

    inline bool isInternalID() const { return getPropertyName() == common::INTERNAL_ID_SUFFIX; }

    inline std::unique_ptr<Expression> copy() const override {
        return make_unique<PropertyExpression>(*this);
    }

private:
    bool isPrimaryKey_ = false;
    std::string propertyName;
    // reference to a node/rel table
    std::string variableName;
    std::unordered_map<common::table_id_t, common::property_id_t> propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
