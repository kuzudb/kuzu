#pragma once

#include "node_rel_expression.h"

namespace kuzu {
namespace binder {

class NodeExpression : public NodeOrRelExpression {
public:
    NodeExpression(common::LogicalType dataType, std::string uniqueName, std::string variableName,
        std::vector<common::table_id_t> tableIDs)
        : NodeOrRelExpression{std::move(dataType), std::move(uniqueName), std::move(variableName),
              std::move(tableIDs)} {}

    void setInternalID(std::unique_ptr<Expression> expression) {
        internalID = std::move(expression);
    }
    std::shared_ptr<Expression> getInternalID() const {
        KU_ASSERT(internalID != nullptr);
        return internalID->copy();
    }

    // Get primary key property expression for a given table ID.
    std::shared_ptr<Expression> getPrimaryKey(common::table_id_t tableID) const;

private:
    std::unique_ptr<Expression> internalID;
};

} // namespace binder
} // namespace kuzu
