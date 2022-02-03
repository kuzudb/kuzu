#pragma once

#include "src/parser/include/parsed_expression.h"

namespace graphflow {
namespace parser {

class ProjectionBody {

public:
    ProjectionBody(bool isDistinct, bool containsStar,
        vector<unique_ptr<ParsedExpression>> projectionExpressions)
        : isDistinct{isDistinct}, containsStar{containsStar}, projectionExpressions{
                                                                  move(projectionExpressions)} {}

    inline void setOrderByExpressions(
        vector<unique_ptr<ParsedExpression>> expressions, vector<bool> sortOrders) {
        orderByExpressions = move(expressions);
        this->isAscOrders = move(sortOrders);
    }
    inline void setSkipExpression(unique_ptr<ParsedExpression> expression) {
        skipExpression = move(expression);
    }
    inline void setLimitExpression(unique_ptr<ParsedExpression> expression) {
        limitExpression = move(expression);
    }

    inline bool isProjectStar() const { return containsStar; }
    inline bool isProjectDistinct() const { return isDistinct; }
    inline const vector<unique_ptr<ParsedExpression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }
    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    inline uint32_t numOrderByExpressions() const { return orderByExpressions.size(); }
    inline const vector<unique_ptr<ParsedExpression>>& getOrderByExpressions() const {
        return orderByExpressions;
    }
    inline const vector<bool>& getSortOrders() const { return isAscOrders; }
    inline bool hasSkipExpression() const { return skipExpression != nullptr; }
    inline ParsedExpression* getSkipExpression() const { return skipExpression.get(); }
    inline bool hasLimitExpression() const { return limitExpression != nullptr; }
    inline ParsedExpression* getLimitExpression() const { return limitExpression.get(); }

private:
    bool isDistinct;
    bool containsStar;
    vector<unique_ptr<ParsedExpression>> projectionExpressions;
    vector<unique_ptr<ParsedExpression>> orderByExpressions;
    vector<bool> isAscOrders;
    unique_ptr<ParsedExpression> skipExpression;
    unique_ptr<ParsedExpression> limitExpression;
};

} // namespace parser
} // namespace graphflow
