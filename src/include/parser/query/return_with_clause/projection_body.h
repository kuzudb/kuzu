#pragma once

#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

class ProjectionBody {
public:
    ProjectionBody(bool isDistinct, bool containsStar_,
        vector<unique_ptr<ParsedExpression>> projectionExpressions)
        : isDistinct{isDistinct}, containsStar_{containsStar_}, projectionExpressions{std::move(
                                                                    projectionExpressions)} {}

    inline bool getIsDistinct() const { return isDistinct; }

    inline bool containsStar() const { return containsStar_; }

    inline const vector<unique_ptr<ParsedExpression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }

    inline void setOrderByExpressions(
        vector<unique_ptr<ParsedExpression>> expressions, vector<bool> sortOrders) {
        orderByExpressions = std::move(expressions);
        isAscOrders = std::move(sortOrders);
    }
    inline bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    inline uint32_t numOrderByExpressions() const { return orderByExpressions.size(); }
    inline const vector<unique_ptr<ParsedExpression>>& getOrderByExpressions() const {
        return orderByExpressions;
    }

    inline vector<bool> getSortOrders() const { return isAscOrders; }

    inline void setSkipExpression(unique_ptr<ParsedExpression> expression) {
        skipExpression = std::move(expression);
    }
    inline bool hasSkipExpression() const { return skipExpression != nullptr; }
    inline ParsedExpression* getSkipExpression() const { return skipExpression.get(); }

    inline void setLimitExpression(unique_ptr<ParsedExpression> expression) {
        limitExpression = std::move(expression);
    }
    inline bool hasLimitExpression() const { return limitExpression != nullptr; }
    inline ParsedExpression* getLimitExpression() const { return limitExpression.get(); }

private:
    bool isDistinct;
    bool containsStar_;
    vector<unique_ptr<ParsedExpression>> projectionExpressions;
    vector<unique_ptr<ParsedExpression>> orderByExpressions;
    vector<bool> isAscOrders;
    unique_ptr<ParsedExpression> skipExpression;
    unique_ptr<ParsedExpression> limitExpression;
};

} // namespace parser
} // namespace kuzu
