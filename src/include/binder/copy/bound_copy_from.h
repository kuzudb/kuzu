#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopyFrom : public BoundStatement {
public:
    BoundCopyFrom(std::unique_ptr<common::CopyDescription> copyDescription,
        catalog::TableSchema* tableSchema, expression_vector columnExpressions,
        std::shared_ptr<Expression> nodeOffsetExpression, bool preservingOrder_)
        : BoundStatement{common::StatementType::COPY_FROM,
              BoundStatementResult::createSingleStringColumnResult()},
          copyDescription{std::move(copyDescription)}, tableSchema{tableSchema},
          columnExpressions{std::move(columnExpressions)},
          nodeOffsetExpression{std::move(nodeOffsetExpression)}, preservingOrder_{
                                                                     preservingOrder_} {}

    inline common::CopyDescription* getCopyDescription() const { return copyDescription.get(); }
    inline catalog::TableSchema* getTableSchema() const { return tableSchema; }
    inline expression_vector getColumnExpressions() const { return columnExpressions; }
    inline std::shared_ptr<Expression> getNodeOffsetExpression() const {
        return nodeOffsetExpression;
    }
    inline bool preservingOrder() const { return preservingOrder_; }

private:
    std::unique_ptr<common::CopyDescription> copyDescription;
    catalog::TableSchema* tableSchema;
    expression_vector columnExpressions;
    std::shared_ptr<Expression> nodeOffsetExpression;
    bool preservingOrder_;
};

} // namespace binder
} // namespace kuzu
