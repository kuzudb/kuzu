#include "src/binder/include/query_binder.h"

#include <fstream>

#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/common/include/assert.h"
#include "src/common/include/csv_reader/csv_reader.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundRegularQuery> QueryBinder::bind(const RegularQuery& regularQuery) {
    auto boundRegularQuery = make_unique<BoundRegularQuery>();
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        boundRegularQuery->addBoundSingleQuery(bindSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    return boundRegularQuery;
}

unique_ptr<BoundSingleQuery> QueryBinder::bindSingleQuery(const SingleQuery& singleQuery) {
    validateFirstMatchIsNotOptional(singleQuery);
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto& queryPart : singleQuery.queryParts) {
        boundSingleQuery->boundQueryParts.push_back(bindQueryPart(*queryPart));
    }
    for (auto& matchStatement : singleQuery.matchStatements) {
        boundSingleQuery->boundMatchStatements.push_back(bindMatchStatement(*matchStatement));
    }
    boundSingleQuery->boundReturnStatement = bindReturnStatement(*singleQuery.returnStatement);
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> QueryBinder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto& matchStatement : queryPart.matchStatements) {
        boundQueryPart->boundMatchStatements.push_back(bindMatchStatement(*matchStatement));
    }
    boundQueryPart->boundWithStatement = bindWithStatement(*queryPart.withStatement);
    return boundQueryPart;
}

unique_ptr<BoundMatchStatement> QueryBinder::bindMatchStatement(
    const MatchStatement& matchStatement) {
    auto prevVariablesInScope = variablesInScope;
    auto queryGraph = bindQueryGraph(matchStatement.graphPattern);
    validateQueryGraphIsConnected(*queryGraph, prevVariablesInScope);
    auto boundMatchStatement =
        make_unique<BoundMatchStatement>(move(queryGraph), matchStatement.isOptional);
    if (matchStatement.whereClause) {
        boundMatchStatement->whereExpression = bindWhereExpression(*matchStatement.whereClause);
    }
    return boundMatchStatement;
}

unique_ptr<BoundWithStatement> QueryBinder::bindWithStatement(const WithStatement& withStatement) {
    auto boundProjectionBody =
        bindProjectionBody(*withStatement.getProjectionBody(), true /* isWithClause */);
    validateOrderByFollowedBySkipOrLimitInWithStatement(*boundProjectionBody);
    auto boundWithStatement = make_unique<BoundWithStatement>(move(boundProjectionBody));
    if (withStatement.hasWhereExpression()) {
        boundWithStatement->setWhereExpression(
            bindWhereExpression(*withStatement.getWhereExpression()));
    }
    return boundWithStatement;
}

unique_ptr<BoundReturnStatement> QueryBinder::bindReturnStatement(
    const ReturnStatement& returnStatement) {
    auto boundReturnStatement = make_unique<BoundReturnStatement>(
        bindProjectionBody(*returnStatement.getProjectionBody(), false /* isWithClause */));
    return boundReturnStatement;
}

unique_ptr<BoundProjectionBody> QueryBinder::bindProjectionBody(
    const ProjectionBody& projectionBody, bool isWithClause) {
    auto projectionExpressions = bindProjectionExpressions(
        projectionBody.getProjectionExpressions(), projectionBody.isProjectStar());
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody.isProjectDistinct(), move(projectionExpressions));
    auto prevVariablesInScope = variablesInScope;
    variablesInScope =
        computeVariablesInScope(boundProjectionBody->getProjectionExpressions(), isWithClause);
    if (projectionBody.hasOrderByExpressions()) {
        boundProjectionBody->setOrderByExpressions(
            bindOrderByExpressions(projectionBody.getOrderByExpressions(), prevVariablesInScope,
                boundProjectionBody->hasAggregationExpressions()),
            projectionBody.getSortOrders());
    }
    if (projectionBody.hasSkipExpression()) {
        boundProjectionBody->setSkipNumber(
            validateAndExtractSkipLimitNumber(*projectionBody.getSkipExpression()));
    }
    if (projectionBody.hasLimitExpression()) {
        boundProjectionBody->setLimitNumber(
            validateAndExtractSkipLimitNumber(*projectionBody.getLimitExpression()));
    }
    return boundProjectionBody;
}

vector<shared_ptr<Expression>> QueryBinder::bindProjectionExpressions(
    const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool isStar) {
    vector<shared_ptr<Expression>> boundProjectionExpressions;
    for (auto& expression : projectionExpressions) {
        boundProjectionExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    if (isStar) {
        if (variablesInScope.empty()) {
            throw invalid_argument(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& [name, expression] : variablesInScope) {
            boundProjectionExpressions.push_back(expression);
        }
    }
    validateProjectionColumnNamesAreUnique(boundProjectionExpressions);
    return boundProjectionExpressions;
}

// Cypher rule of ORDER BY expression scope: if projection contains aggregation, only expressions in
// projection are available. Otherwise, expressions before projection are also available
vector<shared_ptr<Expression>> QueryBinder::bindOrderByExpressions(
    const vector<unique_ptr<ParsedExpression>>& orderByExpressions,
    const unordered_map<string, shared_ptr<Expression>>& prevVariablesInScope,
    bool projectionHasAggregation) {
    auto variablesInScopeBeforeModification = variablesInScope;
    if (!projectionHasAggregation) {
        // restore expressions before projection
        for (auto& [name, variable] : prevVariablesInScope) {
            if (!variablesInScope.contains(name)) {
                variablesInScope.insert({name, variable});
            }
        }
    }
    vector<shared_ptr<Expression>> boundOrderByExpressions;
    for (auto& orderByExpression : orderByExpressions) {
        boundOrderByExpressions.push_back(expressionBinder.bindExpression(*orderByExpression));
    }
    variablesInScope = variablesInScopeBeforeModification;
    return boundOrderByExpressions;
}

unordered_map<string, shared_ptr<Expression>> QueryBinder::computeVariablesInScope(
    const vector<shared_ptr<Expression>>& expressions, bool isWithClause) {
    unordered_map<string, shared_ptr<Expression>> newVariablesInScope;
    for (auto& expression : expressions) {
        auto type = expression->expressionType;
        // Cypher WITH clause special rule: expression except for variable must be aliased
        if (isWithClause && expression->getAlias().empty()) {
            throw invalid_argument("Expression in WITH must be aliased (use AS).");
        }
        newVariablesInScope.insert({expression->getAlias(), expression});
    }
    return newVariablesInScope;
}

shared_ptr<Expression> QueryBinder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    if (BOOL != whereExpression->dataType) {
        throw invalid_argument("Type mismatch: " + whereExpression->getRawName() + " returns " +
                               TypeUtils::dataTypeToString(whereExpression->dataType) +
                               " expected Boolean.");
    }
    return whereExpression;
}

unique_ptr<QueryGraph> QueryBinder::bindQueryGraph(
    const vector<unique_ptr<PatternElement>>& graphPattern) {
    auto queryGraph = make_unique<QueryGraph>();
    for (auto& patternElement : graphPattern) {
        auto leftNode = bindQueryNode(*patternElement->nodePattern, *queryGraph);
        for (auto& patternElementChain : patternElement->patternElementChains) {
            auto rightNode = bindQueryNode(*patternElementChain->nodePattern, *queryGraph);
            bindQueryRel(*patternElementChain->relPattern, leftNode, rightNode, *queryGraph);
            leftNode = rightNode;
        }
    }
    return queryGraph;
}

void QueryBinder::bindQueryRel(const RelPattern& relPattern,
    const shared_ptr<NodeExpression>& leftNode, const shared_ptr<NodeExpression>& rightNode,
    QueryGraph& queryGraph) {
    auto parsedName = relPattern.name;
    if (variablesInScope.contains(parsedName)) {
        auto prevVariable = variablesInScope.at(parsedName);
        if (REL != prevVariable->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   TypeUtils::dataTypeToString(prevVariable->dataType) +
                                   " (expect RELATIONSHIP).");
        } else {
            // Bind to queryRel in scope requires QueryRel takes multiple src & dst nodes
            // Example MATCH (a)-[r1]->(b) MATCH (c)-[r1]->(d)
            // Should be bound as (a,c)-[r1]->(b,d)
            throw invalid_argument("Bind relationship " + parsedName +
                                   " to relationship with same name is not supported.");
        }
    }
    auto relLabel = bindRelLabel(relPattern.label);
    if (ANY_LABEL == relLabel) {
        throw invalid_argument(
            "Any-label is not supported. " + parsedName + " does not have a label.");
    }
    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.arrowDirection;
    validateNodeAndRelLabelIsConnected(relLabel, leftNode->label, isLeftNodeSrc ? FWD : BWD);
    validateNodeAndRelLabelIsConnected(relLabel, rightNode->label, isLeftNodeSrc ? BWD : FWD);
    auto srcNode = isLeftNodeSrc ? leftNode : rightNode;
    auto dstNode = isLeftNodeSrc ? rightNode : leftNode;
    // bind variable length
    auto lowerBound = TypeUtils::convertToInt64(relPattern.lowerBound.c_str());
    auto upperBound = TypeUtils::convertToInt64(relPattern.upperBound.c_str());
    if (lowerBound > upperBound) {
        throw invalid_argument("Lower bound of rel " + parsedName + " is greater than upperBound.");
    }
    auto queryRel = make_shared<RelExpression>(
        getUniqueExpressionName(parsedName), relLabel, srcNode, dstNode, lowerBound, upperBound);
    queryRel->setAlias(parsedName);
    queryRel->setRawName(parsedName);
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryRel});
    }
    queryGraph.addQueryRel(queryRel);
}

shared_ptr<NodeExpression> QueryBinder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph) {
    auto parsedName = nodePattern.name;
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto prevVariable = variablesInScope.at(parsedName);
        if (NODE != prevVariable->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   TypeUtils::dataTypeToString(prevVariable->dataType) +
                                   " (expect NODE).");
        }
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto otherLabel = bindNodeLabel(nodePattern.label);
        GF_ASSERT(queryNode->label != ANY_LABEL);
        if (otherLabel != ANY_LABEL && queryNode->label != otherLabel) {
            throw invalid_argument(
                "Multi-label is not supported. Node " + parsedName + " is given multiple labels.");
        }
    } else { // create new node
        auto nodeLabel = bindNodeLabel(nodePattern.label);
        queryNode = make_shared<NodeExpression>(getUniqueExpressionName(parsedName), nodeLabel);
        queryNode->setAlias(parsedName);
        queryNode->setRawName(parsedName);
        if (ANY_LABEL == nodeLabel) {
            throw invalid_argument(
                "Any-label is not supported. " + parsedName + " does not have a label.");
        }
        if (!parsedName.empty()) {
            variablesInScope.insert({parsedName, queryNode});
        }
    }
    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

label_t QueryBinder::bindRelLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containRelLabel(parsed_label)) {
        throw invalid_argument("Rel label " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromString(parsed_label);
}

label_t QueryBinder::bindNodeLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containNodeLabel(parsed_label)) {
        throw invalid_argument("Node label " + parsed_label + " does not exist.");
    }
    return catalog.getNodeLabelFromString(parsed_label);
}

void QueryBinder::validateFirstMatchIsNotOptional(const SingleQuery& singleQuery) {
    if (singleQuery.hasMatchStatement() && singleQuery.getFirstMatchStatement()->isOptional) {
        throw invalid_argument("First match statement cannot be optional match.");
    }
}

void QueryBinder::validateNodeAndRelLabelIsConnected(
    label_t relLabel, label_t nodeLabel, Direction direction) {
    GF_ASSERT(relLabel != ANY_LABEL);
    GF_ASSERT(nodeLabel != ANY_LABEL);
    auto connectedRelLabels = catalog.getRelLabelsForNodeLabelDirection(nodeLabel, direction);
    for (auto& connectedRelLabel : connectedRelLabels) {
        if (relLabel == connectedRelLabel) {
            return;
        }
    }
    throw invalid_argument("Node label " + catalog.getNodeLabelName(nodeLabel) +
                           " doesn't connect to rel label " + catalog.getRelLabelName(relLabel) +
                           ".");
}

void QueryBinder::validateProjectionColumnNamesAreUnique(
    const vector<shared_ptr<Expression>>& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        if (existColumnNames.contains(expression->getRawName())) {
            throw invalid_argument("Multiple result column with the same name " +
                                   expression->getRawName() + " are not supported.");
        }
        existColumnNames.insert(expression->getRawName());
    }
}

void QueryBinder::validateOrderByFollowedBySkipOrLimitInWithStatement(
    const BoundProjectionBody& boundProjectionBody) {
    auto hasSkipOrLimit = boundProjectionBody.hasSkip() || boundProjectionBody.hasLimit();
    if (boundProjectionBody.hasOrderByExpressions() && !hasSkipOrLimit) {
        throw invalid_argument("In WITH clause, ORDER BY must be followed by SKIP or LIMIT.");
    }
}

void QueryBinder::validateQueryGraphIsConnected(const QueryGraph& queryGraph,
    unordered_map<string, shared_ptr<Expression>> prevVariablesInScope) {
    auto visited = unordered_set<string>();
    for (auto& [name, variable] : prevVariablesInScope) {
        if (NODE == variable->dataType) {
            visited.insert(variable->getUniqueName());
        }
    }
    if (visited.empty()) {
        visited.insert(queryGraph.queryNodes[0]->getUniqueName());
    }
    auto target = visited;
    for (auto& queryNode : queryGraph.queryNodes) {
        target.insert(queryNode->getUniqueName());
    }
    auto frontier = visited;
    while (!frontier.empty()) {
        auto nextFrontier = unordered_set<string>();
        for (auto& nodeInFrontier : frontier) {
            auto nbrs = queryGraph.getNeighbourNodeNames(nodeInFrontier);
            for (auto& nbr : nbrs) {
                if (end(visited) == visited.find(nbr)) {
                    visited.insert(nbr);
                    nextFrontier.insert(nbr);
                }
            }
        }
        if (visited.size() == target.size()) {
            return;
        }
        frontier = nextFrontier;
    }
    throw invalid_argument("Disconnect query graph is not supported.");
}

uint64_t QueryBinder::validateAndExtractSkipLimitNumber(
    const ParsedExpression& skipOrLimitExpression) {
    auto boundExpression = expressionBinder.bindExpression(skipOrLimitExpression);
    GF_ASSERT(boundExpression->expressionType == LITERAL_INT);
    auto skipOrLimitNumber =
        static_pointer_cast<LiteralExpression>(boundExpression)->literal.val.int64Val;
    GF_ASSERT(skipOrLimitNumber >= 0);
    return skipOrLimitNumber;
}

string QueryBinder::getUniqueExpressionName(const string& name) {
    return "_" + to_string(lastExpressionId++) + "_" + name;
}

unordered_map<string, shared_ptr<Expression>> QueryBinder::enterSubquery() {
    return variablesInScope;
}

void QueryBinder::exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope) {
    variablesInScope = move(prevVariablesInScope);
}

} // namespace binder
} // namespace graphflow
