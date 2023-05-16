#pragma once

#include "node_rel_expression.h"
#include "property_expression.h"

namespace kuzu {
namespace binder {

class NodeExpression : public NodeOrRelExpression {
public:
    NodeExpression(
        std::string uniqueName, std::string variableName, std::vector<common::table_id_t> tableIDs)
        : NodeOrRelExpression{common::LogicalType(common::LogicalTypeID::NODE),
              std::move(uniqueName), std::move(variableName), std::move(tableIDs)} {}

    inline void setInternalIDProperty(std::unique_ptr<Expression> expression) {
        internalIDExpression = std::move(expression);
    }
    inline std::shared_ptr<Expression> getInternalIDProperty() const {
        assert(internalIDExpression != nullptr);
        return internalIDExpression->copy();
    }
    inline std::string getInternalIDPropertyName() const {
        return internalIDExpression->getUniqueName();
    }

private:
    std::unique_ptr<Expression> internalIDExpression;
};

} // namespace binder
} // namespace kuzu
