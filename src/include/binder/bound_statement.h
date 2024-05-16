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

    common::StatementType getStatementType() const { return statementType; }

    const BoundStatementResult* getStatementResult() const { return &statementResult; }

    BoundStatementResult* getStatementResultUnsafe() { return &statementResult; }

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const BoundStatement&, const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<BoundStatement&, TARGET&>(*this);
    }

private:
    common::StatementType statementType;
    BoundStatementResult statementResult;
};

} // namespace binder
} // namespace kuzu
