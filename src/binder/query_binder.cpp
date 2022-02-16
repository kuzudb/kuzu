#include "src/binder/include/query_binder.h"

#include "src/binder/expression/include/literal_expression.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundRegularQuery> QueryBinder::bind(const RegularQuery& regularQuery) {
    auto boundRegularQuery = make_unique<BoundRegularQuery>(regularQuery.getIsUnionAll());
    vector<unique_ptr<BoundSingleQuery>> boundSingleQueries;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        boundSingleQueries.push_back(bindSingleQuery(*regularQuery.getSingleQuery(i)));
    }
    validateUnionColumnsOfTheSameType(boundSingleQueries);
    for (auto& boundSingleQuery : boundSingleQueries) {
        boundRegularQuery->addSingleQuery(QueryNormalizer::normalizeQuery(*boundSingleQuery));
    }
    validateIsAllUnionOrUnionAll(*boundRegularQuery);
    return boundRegularQuery;
}

unique_ptr<BoundSingleQuery> QueryBinder::bindSingleQuery(const SingleQuery& singleQuery) {
    validateFirstMatchIsNotOptional(singleQuery);
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        boundSingleQuery->addQueryPart(bindQueryPart(*singleQuery.getQueryPart(i)));
    }
    for (auto i = 0u; i < singleQuery.getNumMatchClauses(); ++i) {
        boundSingleQuery->addMatchClause(bindMatchClause(*singleQuery.getMatchClause(i)));
    }
    boundSingleQuery->setReturnClause(bindReturnClause(*singleQuery.getReturnClause()));
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> QueryBinder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto i = 0u; i < queryPart.getNumMatchClauses(); ++i) {
        boundQueryPart->addMatchClause(bindMatchClause(*queryPart.getMatchClause(i)));
    }
    boundQueryPart->setWithClause(bindWithClause(*queryPart.getWithClause()));
    return boundQueryPart;
}

unique_ptr<BoundMatchClause> QueryBinder::bindMatchClause(const MatchClause& matchClause) {
    auto prevVariablesInScope = variablesInScope;
    auto queryGraph = bindQueryGraph(matchClause.getPatternElements());
    validateQueryGraphIsConnected(*queryGraph, prevVariablesInScope);
    auto boundMatchClause =
        make_unique<BoundMatchClause>(move(queryGraph), matchClause.getIsOptional());
    if (matchClause.hasWhereClause()) {
        boundMatchClause->setWhereExpression(bindWhereExpression(*matchClause.getWhereClause()));
    }
    return boundMatchClause;
}

unique_ptr<BoundWithClause> QueryBinder::bindWithClause(const WithClause& withClause) {
    auto boundProjectionBody =
        bindProjectionBody(*withClause.getProjectionBody(), true /* isWithClause */);
    validateOrderByFollowedBySkipOrLimitInWithClause(*boundProjectionBody);
    auto boundWithClause = make_unique<BoundWithClause>(move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause->setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

unique_ptr<BoundReturnClause> QueryBinder::bindReturnClause(const ReturnClause& returnClause) {
    auto boundReturnClause = make_unique<BoundReturnClause>(
        bindProjectionBody(*returnClause.getProjectionBody(), false /* isWithClause */));
    return boundReturnClause;
}

unique_ptr<BoundProjectionBody> QueryBinder::bindProjectionBody(
    const ProjectionBody& projectionBody, bool isWithClause) {
    auto projectionExpressions = bindProjectionExpressions(
        projectionBody.getProjectionExpressions(), projectionBody.getContainsStar());
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody.getIsDistinct(), move(projectionExpressions));
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
        auto leftNode = bindQueryNode(*patternElement->getFirstNodePattern(), *queryGraph);
        for (auto i = 0u; i < patternElement->getNumPatternElementChains(); ++i) {
            auto patternElementChain = patternElement->getPatternElementChain(i);
            auto rightNode = bindQueryNode(*patternElementChain->getNodePattern(), *queryGraph);
            bindQueryRel(*patternElementChain->getRelPattern(), leftNode, rightNode, *queryGraph);
            leftNode = rightNode;
        }
    }
    return queryGraph;
}

void QueryBinder::bindQueryRel(const RelPattern& relPattern,
    const shared_ptr<NodeExpression>& leftNode, const shared_ptr<NodeExpression>& rightNode,
    QueryGraph& queryGraph) {
    auto parsedName = relPattern.getName();
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
    auto relLabel = bindRelLabel(relPattern.getLabel());
    if (ANY_LABEL == relLabel) {
        throw invalid_argument(
            "Any-label is not supported. " + parsedName + " does not have a label.");
    }
    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.getDirection();
    validateNodeAndRelLabelIsConnected(relLabel, leftNode->getLabel(), isLeftNodeSrc ? FWD : BWD);
    validateNodeAndRelLabelIsConnected(relLabel, rightNode->getLabel(), isLeftNodeSrc ? BWD : FWD);
    auto srcNode = isLeftNodeSrc ? leftNode : rightNode;
    auto dstNode = isLeftNodeSrc ? rightNode : leftNode;
    // bind variable length
    auto lowerBound = TypeUtils::convertToInt64(relPattern.getLowerBound().c_str());
    auto upperBound = TypeUtils::convertToInt64(relPattern.getUpperBound().c_str());
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
    auto parsedName = nodePattern.getName();
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto prevVariable = variablesInScope.at(parsedName);
        if (NODE != prevVariable->dataType) {
            throw invalid_argument(parsedName + " defined with conflicting type " +
                                   TypeUtils::dataTypeToString(prevVariable->dataType) +
                                   " (expect NODE).");
        }
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto otherLabel = bindNodeLabel(nodePattern.getLabel());
        GF_ASSERT(queryNode->getLabel() != ANY_LABEL);
        if (otherLabel != ANY_LABEL && queryNode->getLabel() != otherLabel) {
            throw invalid_argument(
                "Multi-label is not supported. Node " + parsedName + " is given multiple labels.");
        }
    } else { // create new node
        auto nodeLabel = bindNodeLabel(nodePattern.getLabel());
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
    if (singleQuery.isFirstMatchOptional()) {
        throw invalid_argument("First match clause cannot be optional match.");
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

void QueryBinder::validateOrderByFollowedBySkipOrLimitInWithClause(
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
        visited.insert(queryGraph.getQueryNode(0)->getUniqueName());
    }
    auto target = visited;
    for (auto i = 0u; i < queryGraph.getNumQueryNodes(); ++i) {
        target.insert(queryGraph.getQueryNode(i)->getUniqueName());
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

void QueryBinder::validateUnionColumnsOfTheSameType(
    const vector<unique_ptr<BoundSingleQuery>>& boundSingleQueries) {
    if (boundSingleQueries.size() <= 1) {
        return;
    }
    auto expressionsToProject = boundSingleQueries[0]->getExpressionsToReturn();
    for (auto i = 1u; i < boundSingleQueries.size(); i++) {
        auto expressionsToProjectToCheck = boundSingleQueries[i]->getExpressionsToReturn();
        if (expressionsToProject.size() != expressionsToProjectToCheck.size()) {
            throw invalid_argument("The number of columns to union/union all must be the same.");
        }
        // Check whether the dataTypes in union expressions are exactly the same in each single
        // query.
        for (auto j = 0u; j < expressionsToProject.size(); j++) {
            unordered_set<DataType> expectedDataTypes{expressionsToProject[j]->dataType};
            ExpressionBinder::validateExpectedDataType(
                *expressionsToProjectToCheck[j], expectedDataTypes);
        }
    }
}

void QueryBinder::validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery) {
    auto unionAllExpressionCounter = 0u;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries() - 1; i++) {
        unionAllExpressionCounter += regularQuery.getIsUnionAll(i);
    }
    if ((0 < unionAllExpressionCounter) &&
        (unionAllExpressionCounter < regularQuery.getNumSingleQueries() - 1)) {
        throw invalid_argument("Union and union all can't be used together in a query!");
    }
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
