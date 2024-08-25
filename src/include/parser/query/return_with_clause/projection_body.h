#pragma once

#include "common/copy_constructors.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

class ProjectionBody {
public:
    ProjectionBody(bool isDistinct,
        std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions)
        : isDistinct{isDistinct}, projectionExpressions{std::move(projectionExpressions)} {}
    DELETE_COPY_DEFAULT_MOVE(ProjectionBody);

    bool getIsDistinct() const { return isDistinct; }

    const std::vector<std::unique_ptr<ParsedExpression>>& getProjectionExpressions() const {
        return projectionExpressions;
    }

    void setOrderByExpressions(std::vector<std::unique_ptr<ParsedExpression>> expressions,
        std::vector<bool> sortOrders) {
        orderByExpressions = std::move(expressions);
        isAscOrders = std::move(sortOrders);
    }
    bool hasOrderByExpressions() const { return !orderByExpressions.empty(); }
    const std::vector<std::unique_ptr<ParsedExpression>>& getOrderByExpressions() const {
        return orderByExpressions;
    }

    std::vector<bool> getSortOrders() const { return isAscOrders; }

    void setSkipExpression(std::unique_ptr<ParsedExpression> expression) {
        skipExpression = std::move(expression);
    }
    bool hasSkipExpression() const { return skipExpression != nullptr; }
    ParsedExpression* getSkipExpression() const { return skipExpression.get(); }

    void setLimitExpression(std::unique_ptr<ParsedExpression> expression) {
        limitExpression = std::move(expression);
    }
    bool hasLimitExpression() const { return limitExpression != nullptr; }
    ParsedExpression* getLimitExpression() const { return limitExpression.get(); }

private:
    bool isDistinct;
    std::vector<std::unique_ptr<ParsedExpression>> projectionExpressions;
    std::vector<std::unique_ptr<ParsedExpression>> orderByExpressions;
    std::vector<bool> isAscOrders;
    std::unique_ptr<ParsedExpression> skipExpression;
    std::unique_ptr<ParsedExpression> limitExpression;
};

} // namespace parser
} // namespace kuzu
