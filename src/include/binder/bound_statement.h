#pragma once

#include "bound_statement_result.h"
#include "common/copy_constructors.h"
#include "common/enums/statement_type.h"

namespace kuzu {
namespace binder {

class BoundStatement {
public:
    BoundStatement(common::StatementType statementType, BoundStatementResult statementResult)
        : statementType{statementType}, statementResult{std::move(statementResult)} {}
    DELETE_COPY_DEFAULT_MOVE(BoundStatement);

    virtual ~BoundStatement() = default;

    inline common::StatementType getStatementType() const { return statementType; }

    inline const BoundStatementResult* getStatementResult() const { return &statementResult; }

private:
    common::StatementType statementType;
    BoundStatementResult statementResult;
};

} // namespace binder
} // namespace kuzu
