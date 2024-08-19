#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedExpressionVisitor {
public:
    virtual ~ParsedExpressionVisitor() = default;

    void visit(const ParsedExpression* expr);

    void visitSwitch(const ParsedExpression* expr);
    virtual void visitFunctionExpr(const ParsedExpression*) {}
    virtual void visitAggFunctionExpr(const ParsedExpression*) {}
    virtual void visitPropertyExpr(const ParsedExpression*) {}
    virtual void visitLiteralExpr(const ParsedExpression*) {}
    virtual void visitVariableExpr(const ParsedExpression*) {}
    virtual void visitPathExpr(const ParsedExpression*) {}
    virtual void visitNodeRelExpr(const ParsedExpression*) {}
    virtual void visitParamExpr(const ParsedExpression*) {}
    virtual void visitSubqueryExpr(const ParsedExpression*) {}
    virtual void visitCaseExpr(const ParsedExpression*) {}
    virtual void visitGraphExpr(const ParsedExpression*) {}
    virtual void visitLambdaExpr(const ParsedExpression*) {}
    virtual void visitStar(const ParsedExpression*) {}
    virtual void visitQuantifierExpr(const ParsedExpression*) {}

    void visitChildren(const ParsedExpression& expr);
    void visitCaseExprChildren(const ParsedExpression& expr);
};

class ParsedParamExprCollector : public ParsedExpressionVisitor {
public:
    std::vector<const ParsedExpression*> getParamExprs() const { return paramExprs; }
    bool hasParamExprs() const { return !paramExprs.empty(); }

    void visitParamExpr(const ParsedExpression* expr) override { paramExprs.push_back(expr); }

private:
    std::vector<const ParsedExpression*> paramExprs;
};

class ParsedSequenceFunctionCollector : public ParsedExpressionVisitor {
public:
    bool hasSeqUpdate() const { return hasSeqUpdate_; }
    void visitFunctionExpr(const ParsedExpression* expr) override;

private:
    bool hasSeqUpdate_ = false;
};

} // namespace parser
} // namespace kuzu
