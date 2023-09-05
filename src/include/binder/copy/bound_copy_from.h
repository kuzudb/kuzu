#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

struct BoundCopyFromInfo {
    std::unique_ptr<common::CopyDescription> copyDesc;
    catalog::TableSchema* tableSchema;
    expression_vector columnExpressions;
    std::shared_ptr<Expression> offsetExpression;
    // `boundOffsetExpression` and `nbrOffsetExpression` are for rel tables only.
    std::shared_ptr<Expression> boundOffsetExpression;
    std::shared_ptr<Expression> nbrOffsetExpression;

    bool containsSerial;

    BoundCopyFromInfo(std::unique_ptr<common::CopyDescription> copyDesc,
        catalog::TableSchema* tableSchema, expression_vector columnExpressions,
        std::shared_ptr<Expression> offsetExpression,
        std::shared_ptr<Expression> boundOffsetExpression,
        std::shared_ptr<Expression> nbrOffsetExpression, bool containsSerial)
        : copyDesc{std::move(copyDesc)}, tableSchema{tableSchema}, columnExpressions{std::move(
                                                                       columnExpressions)},
          offsetExpression{std::move(offsetExpression)}, boundOffsetExpression{std::move(
                                                             boundOffsetExpression)},
          nbrOffsetExpression{std::move(nbrOffsetExpression)}, containsSerial{containsSerial} {}

    std::unique_ptr<BoundCopyFromInfo> copy();
};

class BoundCopyFrom : public BoundStatement {
public:
    explicit BoundCopyFrom(std::unique_ptr<BoundCopyFromInfo> info)
        : BoundStatement{common::StatementType::COPY_FROM,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    inline BoundCopyFromInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<BoundCopyFromInfo> info;
};

} // namespace binder
} // namespace kuzu
