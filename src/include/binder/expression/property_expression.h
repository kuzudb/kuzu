#pragma once

#include "common/configs.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(DataType dataType, const string& propertyName, const Expression& nodeOrRel,
        unordered_map<table_id_t, property_id_t> propertyIDPerTable, bool isPrimaryKey_)
        : Expression{PROPERTY, std::move(dataType), nodeOrRel.getUniqueName() + "." + propertyName},
          isPrimaryKey_{isPrimaryKey_}, propertyName{propertyName},
          variableName{nodeOrRel.getUniqueName()}, propertyIDPerTable{
                                                       std::move(propertyIDPerTable)} {
        rawName = nodeOrRel.getRawName() + "." + propertyName;
    }
    PropertyExpression(const PropertyExpression& other)
        : Expression{PROPERTY, other.dataType, other.uniqueName},
          isPrimaryKey_{other.isPrimaryKey_}, propertyName{other.propertyName},
          variableName{other.variableName}, propertyIDPerTable{other.propertyIDPerTable} {
        rawName = other.rawName;
    }

    inline bool isPrimaryKey() const { return isPrimaryKey_; }

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
    bool isPrimaryKey_ = false;
    string propertyName;
    // reference to a node/rel table
    string variableName;
    unordered_map<table_id_t, property_id_t> propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
