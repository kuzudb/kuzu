#pragma once

#include "bound_statement_result.h"
#include "common/statement_type.h"

namespace kuzu {
namespace binder {

class BoundStatement {
public:
    explicit BoundStatement(
        common::StatementType statementType, std::unique_ptr<BoundStatementResult> statementResult)
        : statementType{statementType}, statementResult{std::move(statementResult)} {}

    virtual ~BoundStatement() = default;

    inline common::StatementType getStatementType() const { return statementType; }

    inline BoundStatementResult* getStatementResult() const { return statementResult.get(); }

    inline bool isDDL() const { return common::StatementTypeUtils::isDDL(statementType); }
    inline bool isCopyCSV() const { return common::StatementTypeUtils::isCopyCSV(statementType); }

    virtual inline bool isReadOnly() const { return false; }

private:
    common::StatementType statementType;
    std::unique_ptr<BoundStatementResult> statementResult;
};

} // namespace binder
} // namespace kuzu
