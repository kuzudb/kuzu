#pragma once

#include "common/configs.h"
#include "expression.h"

namespace kuzu {
namespace binder {

class PropertyExpression : public Expression {
public:
    PropertyExpression(DataType dataType, const string& propertyName,
        unordered_map<table_id_t, property_id_t> propertyIDPerTable,
        const shared_ptr<Expression>& child)
        : Expression{PROPERTY, std::move(dataType), child,
              child->getUniqueName() + "." + propertyName},
          propertyName{propertyName}, propertyIDPerTable{std::move(propertyIDPerTable)} {}

    inline string getPropertyName() const { return propertyName; }

    inline bool hasPropertyID(table_id_t tableID) const {
        return propertyIDPerTable.contains(tableID);
    }
    inline property_id_t getPropertyID(table_id_t tableID) const {
        assert(propertyIDPerTable.contains(tableID));
        return propertyIDPerTable.at(tableID);
    }

    inline bool isInternalID() const { return getPropertyName() == INTERNAL_ID_SUFFIX; }

private:
    string propertyName;
    unordered_map<table_id_t, property_id_t> propertyIDPerTable;
};

} // namespace binder
} // namespace kuzu
