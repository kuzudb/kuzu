#pragma once

#include "node_rel_expression.h"
#include "property_expression.h"

namespace kuzu {
namespace binder {

class NodeExpression : public NodeOrRelExpression {
public:
    NodeExpression(const string& uniqueName, vector<table_id_t> tableIDs)
        : NodeOrRelExpression{NODE, uniqueName, std::move(tableIDs)} {}

    inline void setInternalIDProperty(shared_ptr<Expression> expression) {
        internalIDExpression = std::move(expression);
    }
    inline shared_ptr<Expression> getInternalIDProperty() const {
        assert(internalIDExpression != nullptr);
        return internalIDExpression;
    }
    inline string getInternalIDPropertyName() const {
        return internalIDExpression->getUniqueName();
    }

private:
    shared_ptr<Expression> internalIDExpression;
};

} // namespace binder
} // namespace kuzu
