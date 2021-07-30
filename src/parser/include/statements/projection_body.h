#pragma once

#include "src/parser/include/parsed_expression.h"

namespace graphflow {
namespace parser {

class ProjectionBody {

public:
    ProjectionBody(bool containsStar, vector<unique_ptr<ParsedExpression>> projectionExpressions)
        : containsStar{containsStar}, projectionExpressions{move(projectionExpressions)} {}

    inline void setLimitExpression(unique_ptr<ParsedExpression> expression) {
        limitExpression = move(expression);
    }

    inline bool isProjectStar() const { return containsStar; }
    inline const vector<unique_ptr<ParsedExpression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }
    inline bool hasLimitExpression() const { return limitExpression != nullptr; }
    inline ParsedExpression* getLimitExpression() const { return limitExpression.get(); }

private:
    bool containsStar;
    vector<unique_ptr<ParsedExpression>> projectionExpressions;
    unique_ptr<ParsedExpression> limitExpression;
};

} // namespace parser
} // namespace graphflow
