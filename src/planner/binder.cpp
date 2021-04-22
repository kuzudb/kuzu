#include "src/planner/include/binder.h"

namespace graphflow {
namespace planner {

static string makeUniqueVariableName(const string& name, uint32_t& idx) {
    return name + "_gf" + to_string(idx++);
}

static bool isAnonymousVariable(const string& variableName) {
    return variableName.substr(0, 3) == "_gf";
}

static void validateProjectionColumnNamesAreUnique(
    const vector<shared_ptr<LogicalExpression>>& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        auto k = *expression;
        if (end(existColumnNames) != existColumnNames.find(expression->getName())) {
            throw invalid_argument("Duplicate return column name: " + expression->getName());
        }
        existColumnNames.insert(expression->getName());
    }
}

static void validateOnlyFunctionIsCountStar(vector<shared_ptr<LogicalExpression>>& expressions) {
    for (auto& expression : expressions) {
        if (FUNCTION == expression->expressionType && COUNT_STAR == expression->variableName &&
            1 != expressions.size()) {
            throw invalid_argument("The only function in the return clause should be COUNT(*).");
        }
    }
}

static void validateQueryGraphIsConnected(
    const QueryGraph& queryGraph, const unordered_map<string, QueryNode*>& queryNodesInScope) {
    auto visited = unordered_set<string>();
    for (auto& [name, queryNode] : queryNodesInScope) {
        visited.insert(queryNode->name);
    }
    if (visited.empty()) {
        visited.insert(queryGraph.queryNodes[0]->name);
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
        if (visited.size() == queryNodesInScope.size() + queryGraph.queryNodes.size()) {
            return;
        }
        frontier = nextFrontier;
    }
    throw invalid_argument("Disconnect query graph is not supported.");
}

// This function should be removed once QNode, QRel becomes LogicalExpression
static vector<shared_ptr<LogicalExpression>> getAllVariablesInScopeAsExpressions(
    const VariablesInScope& variablesInScope) {
    auto expressions = vector<shared_ptr<LogicalExpression>>();
    for (auto& [name, queryNode] : variablesInScope.nameToQueryNodeMap) {
        if (isAnonymousVariable(name)) {
            continue;
        }
        auto node = make_shared<LogicalExpression>(VARIABLE, NODE, queryNode->name);
        node->alias = name;
        expressions.push_back(node);
    }
    for (auto& [name, queryRel] : variablesInScope.nameToQueryRelMap) {
        if (isAnonymousVariable(name)) {
            continue;
        }
        auto rel = make_shared<LogicalExpression>(VARIABLE, REL, queryRel->name);
        rel->alias = name;
        expressions.push_back(rel);
    }
    for (auto& [name, expression] : variablesInScope.nameToExpressionMap) {
        expressions.push_back(expression);
    }
    return expressions;
}

unique_ptr<BoundSingleQuery> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto& parsedQueryPart : singleQuery.queryParts) {
        auto boundQueryPart = make_unique<BoundQueryPart>();
        if (!parsedQueryPart->matchStatements.empty()) {
            boundQueryPart->boundMatchStatement =
                bindMatchStatementsAndMerge(parsedQueryPart->matchStatements);
        }
        boundQueryPart->boundWithStatement = bindWithStatement(*parsedQueryPart->withStatement);
        boundSingleQuery->boundQueryParts.push_back(move(boundQueryPart));
    }
    if (!singleQuery.matchStatements.empty()) {
        boundSingleQuery->boundMatchStatement =
            bindMatchStatementsAndMerge(singleQuery.matchStatements);
    }
    boundSingleQuery->boundReturnStatement = bindReturnStatement(*singleQuery.returnStatement);
    return boundSingleQuery;
}

unique_ptr<BoundMatchStatement> Binder::bindMatchStatementsAndMerge(
    const vector<unique_ptr<MatchStatement>>& matchStatements) {
    unique_ptr<BoundMatchStatement> mergedMatchStatement;
    for (auto& matchStatement : matchStatements) {
        if (!mergedMatchStatement) {
            mergedMatchStatement = bindMatchStatement(*matchStatement);
        } else {
            mergedMatchStatement->merge(*bindMatchStatement(*matchStatement));
        }
    }
    return mergedMatchStatement;
}

unique_ptr<BoundMatchStatement> Binder::bindMatchStatement(const MatchStatement& matchStatement) {
    auto prevVariablesInScope = variablesInScope.nameToQueryNodeMap;
    auto queryGraph = bindQueryGraph(matchStatement.graphPattern);
    validateQueryGraphIsConnected(*queryGraph, prevVariablesInScope);
    auto boundMatchStatement = make_unique<BoundMatchStatement>(move(queryGraph));
    if (matchStatement.whereClause) {
        boundMatchStatement->whereExpression = bindWhereExpression(*matchStatement.whereClause);
    }
    return boundMatchStatement;
}

unique_ptr<BoundWithStatement> Binder::bindWithStatement(const WithStatement& withStatement) {
    auto expressionsToProject =
        bindProjectExpressions(withStatement.expressions, withStatement.containsStar);
    auto newVariablesInScope =
        withStatement.containsStar ? variablesInScope.copy() : VariablesInScope();
    // if else statement should go away once we have QNode, QRel as LogicalExpression
    for (auto& expression : expressionsToProject) {
        if (NODE == expression->dataType) {
            auto nodeName = expression->alias;
            newVariablesInScope.addQueryNode(nodeName, variablesInScope.getQueryNode(nodeName));
        } else if (REL == expression->dataType) {
            auto relName = expression->alias;
            newVariablesInScope.addQueryRel(relName, variablesInScope.getQueryRel(relName));
        } else {
            newVariablesInScope.addExpression(expression->getName(), expression);
        }
    }
    variablesInScope = newVariablesInScope;
    auto boundWithStatement = make_unique<BoundWithStatement>(move(expressionsToProject));
    if (withStatement.whereClause) {
        boundWithStatement->whereExpression = bindWhereExpression(*withStatement.whereClause);
    }
    return boundWithStatement;
}

unique_ptr<BoundReturnStatement> Binder::bindReturnStatement(
    const ReturnStatement& returnStatement) {
    return make_unique<BoundReturnStatement>(
        bindProjectExpressions(returnStatement.expressions, returnStatement.containsStar));
}

shared_ptr<LogicalExpression> Binder::bindWhereExpression(
    const ParsedExpression& parsedExpression) {
    auto expressionBinder = make_unique<ExpressionBinder>(variablesInScope, catalog);
    auto whereExpression = expressionBinder->bindExpression(parsedExpression);
    if (BOOL != whereExpression->dataType) {
        throw invalid_argument("Type mismatch: " + whereExpression->rawExpression + " returns " +
                               dataTypeToString(whereExpression->dataType) + " expected Boolean.");
    }
    return whereExpression;
}

vector<shared_ptr<LogicalExpression>> Binder::bindProjectExpressions(
    const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar) {
    auto expressionBinder = make_unique<ExpressionBinder>(variablesInScope, catalog);
    auto expressions = projectStar ? getAllVariablesInScopeAsExpressions(variablesInScope) :
                                     vector<shared_ptr<LogicalExpression>>();
    for (auto& parsedExpression : parsedExpressions) {
        expressions.push_back(expressionBinder->bindExpression(*parsedExpression));
    }
    validateProjectionColumnNamesAreUnique(expressions);
    validateOnlyFunctionIsCountStar(expressions);
    return expressions;
}

unique_ptr<QueryGraph> Binder::bindQueryGraph(
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

void Binder::bindQueryRel(const RelPattern& relPattern, QueryNode* leftNode, QueryNode* rightNode,
    QueryGraph& queryGraph) {
    auto parsedName = relPattern.name;
    // should be one conflict naming check once QNode, QRel becomes LogicalExpression
    if (variablesInScope.containsQueryNode(parsedName)) {
        throw invalid_argument(parsedName + " is defined as node (expect relationship).");
    }
    if (variablesInScope.containsExpression(parsedName)) {
        auto expression = variablesInScope.getExpression(parsedName);
        throw invalid_argument(parsedName + " is defined as " +
                               expressionTypeToString(expression->expressionType) +
                               " expression (expect relationship).");
    }
    // Bind to rel in scope requires QueryRel takes multiple src & dst nodes
    // Example MATCH (a)-[r1]->(b) MATCH (c)-[r1]->(d)
    // Should be bound as (a,c)-[r1]->(b,d)
    if (variablesInScope.containsQueryRel(parsedName)) {
        throw invalid_argument("Bind relationship " + parsedName +
                               " to relationship with same name is not supported.");
    }
    auto rel = make_unique<QueryRel>(
        makeUniqueVariableName(parsedName, lastVariableIdx), bindRelLabel(relPattern.label));
    auto isLeftNodeSrc = RIGHT == relPattern.arrowDirection;
    bindNodeToRel(*rel, leftNode, isLeftNodeSrc);
    bindNodeToRel(*rel, rightNode, !isLeftNodeSrc);
    if (!parsedName.empty()) {
        variablesInScope.addQueryRel(parsedName, rel.get());
    }
    queryGraph.addQueryRel(move(rel));
}

void Binder::bindNodeToRel(QueryRel& queryRel, QueryNode* queryNode, bool isSrcNode) {
    if (ANY_LABEL != queryNode->label && ANY_LABEL != queryRel.label) {
        auto relLabels =
            catalog.getRelLabelsForNodeLabelDirection(queryNode->label, isSrcNode ? FWD : BWD);
        for (auto& relLabel : relLabels) {
            if (relLabel == queryRel.label) {
                isSrcNode ? queryRel.srcNode = queryNode : queryRel.dstNode = queryNode;
                return;
            }
        }
        throw invalid_argument("Node: " + queryNode->name +
                               " doesn't connect to edge with same type as: " + queryRel.name);
    }
    isSrcNode ? queryRel.srcNode = queryNode : queryRel.dstNode = queryNode;
}

QueryNode* Binder::bindQueryNode(const NodePattern& nodePattern, QueryGraph& queryGraph) {
    auto parsedName = nodePattern.name;
    // should be one conflict naming check once QNode, QRel becomes LogicalExpression
    if (variablesInScope.containsQueryRel(parsedName)) {
        throw invalid_argument(parsedName + " is defined as relationship (expect node).");
    }
    if (variablesInScope.containsExpression(parsedName)) {
        auto expression = variablesInScope.getExpression(parsedName);
        throw invalid_argument(parsedName + " is defined as " +
                               expressionTypeToString(expression->expressionType) +
                               " expression (expect node).");
    }
    // bind to node in scope
    if (variablesInScope.containsQueryNode(parsedName)) {
        auto nodeInScope = variablesInScope.getQueryNode(parsedName);
        auto otherLabel = bindNodeLabel(nodePattern.label);
        if (ANY_LABEL == nodeInScope->label) {
            nodeInScope->label = otherLabel;
            return nodeInScope;
        } else {
            if (ANY_LABEL != otherLabel && nodeInScope->label != otherLabel) {
                throw invalid_argument("Multi-label is not supported. Node (" + parsedName +
                                       ") is given multiple labels.");
            }
        }
        return nodeInScope;
    }
    auto node = make_unique<QueryNode>(
        makeUniqueVariableName(parsedName, lastVariableIdx), bindNodeLabel(nodePattern.label));
    auto nodePtr = node.get();
    queryGraph.addQueryNode(move(node));
    if (!parsedName.empty()) {
        variablesInScope.addQueryNode(parsedName, nodePtr);
    }
    return nodePtr;
}

label_t Binder::bindRelLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containRelLabel(parsed_label.c_str())) {
        throw invalid_argument("Rel label: " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromString(parsed_label.c_str());
}

label_t Binder::bindNodeLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containNodeLabel(parsed_label.c_str())) {
        throw invalid_argument("Node label: " + parsed_label + " does not exist.");
    }
    return catalog.getNodeLabelFromString(parsed_label.c_str());
}

} // namespace planner
} // namespace graphflow
