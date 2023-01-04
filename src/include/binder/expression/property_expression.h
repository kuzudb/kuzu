#pragma once

#include "common/configs.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(DataType dataType, const string& propertyName, const Expression& nodeOrRel,
        unordered_map<table_id_t, property_id_t> propertyIDPerTable)
        : Expression{PROPERTY, std::move(dataType), nodeOrRel.getUniqueName() + "." + propertyName},
          propertyName{propertyName}, variableName{nodeOrRel.getUniqueName()},
          propertyIDPerTable{std::move(propertyIDPerTable)} {
        rawName = nodeOrRel.getRawName() + "." + propertyName;
    }
    PropertyExpression(const PropertyExpression& other)
        : Expression{PROPERTY, other.dataType, other.uniqueName}, propertyName{other.propertyName},
          variableName{other.variableName}, propertyIDPerTable{other.propertyIDPerTable} {
        rawName = other.rawName;
    }

    inline string getPropertyName() const { return propertyName; }

    inline string getVariableName() const { return variableName; }

    inline bool hasPropertyID(table_id_t tableID) const {
        return propertyIDPerTable.contains(tableID);
    }
    inline property_id_t getPropertyID(table_id_t tableID) const {
        assert(propertyIDPerTable.contains(tableID));
        return propertyIDPerTable.at(tableID);
    }

    inline bool isInternalID() const { return getPropertyName() == INTERNAL_ID_SUFFIX; }

    inline unique_ptr<Expression> copy() const override {
        return make_unique<PropertyExpression>(*this);
    }

private:
    string propertyName;
    // reference to a node/rel table
    string variableName;
    unordered_map<table_id_t, property_id_t> propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
