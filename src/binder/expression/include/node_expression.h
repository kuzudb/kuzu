#pragma once

#include "property_expression.h"

namespace graphflow {
namespace binder {

class NodeExpression : public Expression {

public:
    NodeExpression(const string& uniqueName, table_id_t tableID)
        : Expression{VARIABLE, NODE, uniqueName}, tableID{tableID} {}

    inline table_id_t getTableID() const { return tableID; }

    inline string getIDProperty() const { return uniqueName + "." + INTERNAL_ID_SUFFIX; }

    inline shared_ptr<Expression> getNodeIDPropertyExpression() {
        return make_unique<PropertyExpression>(DataType(NODE_ID), INTERNAL_ID_SUFFIX,
            UINT32_MAX /* property key for internal id*/, shared_from_this());
    }

private:
    table_id_t tableID;
};

} // namespace binder
} // namespace graphflow
