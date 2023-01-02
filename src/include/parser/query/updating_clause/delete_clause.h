#pragma once

#include "parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class DeleteClause : public UpdatingClause {
public:
    DeleteClause() : UpdatingClause{ClauseType::DELETE} {};
    ~DeleteClause() override = default;

    inline void addExpression(unique_ptr<ParsedExpression> expression) {
        expressions.push_back(move(expression));
    }
    inline uint32_t getNumExpressions() const { return expressions.size(); }
    inline ParsedExpression* getExpression(uint32_t idx) const { return expressions[idx].get(); }

private:
    vector<unique_ptr<ParsedExpression>> expressions;
};

} // namespace parser
} // namespace kuzu
