#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundExplain : public BoundStatement {
public:
    explicit BoundExplain(std::unique_ptr<BoundStatement> statementToExplain)
        : BoundStatement{common::StatementType::EXPLAIN,
              BoundStatementResult::createSingleStringColumnResult()},
          statementToExplain{std::move(statementToExplain)} {}

    inline BoundStatement* getStatementToExplain() const { return statementToExplain.get(); }

private:
    std::unique_ptr<BoundStatement> statementToExplain;
};

} // namespace binder
} // namespace kuzu
