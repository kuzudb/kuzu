#pragma once

#include "node_rel_expression.h"

namespace kuzu {
namespace binder {

class KUZU_API NodeExpression final : public NodeOrRelExpression {
public:
    NodeExpression(common::LogicalType dataType, std::string uniqueName, std::string variableName,
        std::vector<catalog::TableCatalogEntry*> entries)
        : NodeOrRelExpression{std::move(dataType), std::move(uniqueName), std::move(variableName),
              std::move(entries)} {}

    ~NodeExpression() override;

    bool isMultiLabeled() const override { return entries.size() > 1; }

    void setInternalID(std::unique_ptr<Expression> expression) {
        internalID = std::move(expression);
    }
    std::shared_ptr<Expression> getInternalID() const {
        KU_ASSERT(internalID != nullptr);
        return internalID->copy();
    }

    // Get the primary key property expression for a given table ID.
    std::shared_ptr<Expression> getPrimaryKey(common::table_id_t tableID) const;

private:
    std::unique_ptr<Expression> internalID;
};

} // namespace binder
} // namespace kuzu
