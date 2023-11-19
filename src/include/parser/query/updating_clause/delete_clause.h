#pragma once

#include "parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class DeleteClause final : public UpdatingClause {
public:
    DeleteClause(common::DeleteClauseType deleteClauseType)
        : UpdatingClause{common::ClauseType::DELETE_}, deleteClauseType{deleteClauseType} {};

    inline void addExpression(std::unique_ptr<ParsedExpression> expression) {
        expressions.push_back(std::move(expression));
    }
    inline common::DeleteClauseType getDeleteClauseType() const { return deleteClauseType; }
    inline uint32_t getNumExpressions() const { return expressions.size(); }
    inline ParsedExpression* getExpression(uint32_t idx) const { return expressions[idx].get(); }

private:
    common::DeleteClauseType deleteClauseType;
    std::vector<std::unique_ptr<ParsedExpression>> expressions;
};

} // namespace parser
} // namespace kuzu
