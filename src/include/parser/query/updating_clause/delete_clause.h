#pragma once

#include "parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class DeleteClause : public UpdatingClause {
public:
    DeleteClause() : UpdatingClause{common::ClauseType::DELETE_} {};
    ~DeleteClause() override = default;

    inline void addExpression(std::unique_ptr<ParsedExpression> expression) {
        expressions.push_back(std::move(expression));
    }
    inline uint32_t getNumExpressions() const { return expressions.size(); }
    inline ParsedExpression* getExpression(uint32_t idx) const { return expressions[idx].get(); }

private:
    std::vector<std::unique_ptr<ParsedExpression>> expressions;
};

} // namespace parser
} // namespace kuzu
