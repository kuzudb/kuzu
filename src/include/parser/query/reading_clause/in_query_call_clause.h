#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/project_graph.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCallClause final : public ReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::IN_QUERY_CALL;

public:
    explicit InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpression)
        : ReadingClause{clauseType_}, functionExpression{std::move(functionExpression)} {}

    const ParsedExpression* getFunctionExpression() const { return functionExpression.get(); }

    void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    bool hasWherePredicate() const { return wherePredicate != nullptr; }
    const ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

    void setProjectGraph(std::unique_ptr<ProjectGraph> projectGraph_) {
        projectGraph = std::move(projectGraph_);
    }
    bool hasProjectGraph() const { return projectGraph != nullptr; }
    const ProjectGraph* getProjectGraph() const { return projectGraph.get(); }

private:
    std::unique_ptr<ParsedExpression> functionExpression;
    std::unique_ptr<ParsedExpression> wherePredicate;
    std::unique_ptr<ProjectGraph> projectGraph;
};

} // namespace parser
} // namespace kuzu
