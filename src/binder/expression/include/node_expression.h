#pragma once

#include "property_expression.h"

namespace graphflow {
namespace binder {

const string INTERNAL_ID_SUFFIX = "_id";

class NodeExpression : public Expression {

public:
    NodeExpression(const string& uniqueName, label_t label)
        : Expression{VARIABLE, NODE, uniqueName}, label{label} {}

    inline label_t getLabel() const { return label; }

    inline string getIDProperty() const { return uniqueName + "." + INTERNAL_ID_SUFFIX; }

    inline shared_ptr<Expression> getNodeIDPropertyExpression() {
        return make_unique<PropertyExpression>(NODE_ID, INTERNAL_ID_SUFFIX,
            UINT32_MAX /* property key for internal id*/, shared_from_this());
    }

private:
    label_t label;
};

} // namespace binder
} // namespace graphflow
