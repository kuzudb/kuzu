#include "src/binder/include/query_binder.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/common/include/type_utils.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundRegularQuery> QueryBinder::bind(const RegularQuery& regularQuery) {
    auto boundRegularQuery = make_unique<BoundRegularQuery>(regularQuery.getIsUnionAll());
    vector<unique_ptr<BoundSingleQuery>> boundSingleQueries;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); i++) {
        // Don't clear scope within bindSingleQuery() yet because it is also used for subquery
        // binding.
        variablesInScope.clear();
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
    for (auto i = 0u; i < singleQuery.getNumSetClauses(); ++i) {
        boundSingleQuery->addSetClause(bindSetClause(*singleQuery.getSetClause(i)));
    }
    if (singleQuery.hasReturnClause()) {
        boundSingleQuery->setReturnClause(bindReturnClause(*singleQuery.getReturnClause()));
    }
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> QueryBinder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto i = 0u; i < queryPart.getNumMatchClauses(); ++i) {
        boundQueryPart->addMatchClause(bindMatchClause(*queryPart.getMatchClause(i)));
    }
    for (auto i = 0u; i < queryPart.getNumSetClauses(); ++i) {
        boundQueryPart->addSetClause(bindSetClause(*queryPart.getSetClause(i)));
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

unique_ptr<BoundSetClause> QueryBinder::bindSetClause(const SetClause& setClause) {
    auto boundSetClause = make_unique<BoundSetClause>();
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto boundOrigin = expressionBinder.bindExpression(*setItem->origin);
        auto boundTarget = expressionBinder.bindExpression(*setItem->target);
        ExpressionBinder::implicitCastIfNecessary(boundTarget, boundOrigin->dataType.typeID);
        auto boundSetItem = make_unique<BoundSetItem>(move(boundOrigin), move(boundTarget));
        boundSetClause->addSetItem(move(boundSetItem));
    }
    return boundSetClause;
}

unique_ptr<BoundWithClause> QueryBinder::bindWithClause(const WithClause& withClause) {
    auto projectionBody = withClause.getProjectionBody();
    auto boundProjectionExpressions = bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->getContainsStar());
    validateProjectionColumnsInWithClauseAreAliased(boundProjectionExpressions);
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    validateOrderByFollowedBySkipOrLimitInWithClause(*boundProjectionBody);
    variablesInScope.clear();
    addExpressionsToScope(boundProjectionBody->getProjectionExpressions());
    auto boundWithClause = make_unique<BoundWithClause>(move(boundProjectionBody));
    if (withClause.hasWhereExpression()) {
        boundWithClause->setWhereExpression(bindWhereExpression(*withClause.getWhereExpression()));
    }
    return boundWithClause;
}

unique_ptr<BoundReturnClause> QueryBinder::bindReturnClause(const ReturnClause& returnClause) {
    auto projectionBody = returnClause.getProjectionBody();
    auto boundProjectionExpressions = rewriteProjectionExpressions(bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->getContainsStar()));
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    return make_unique<BoundReturnClause>(move(boundProjectionBody));
}

expression_vector QueryBinder::bindProjectionExpressions(
    const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool containsStar) {
    expression_vector boundProjectionExpressions;
    for (auto& expression : projectionExpressions) {
        boundProjectionExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    if (containsStar) {
        if (variablesInScope.empty()) {
            throw BinderException(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& [name, expression] : variablesInScope) {
            boundProjectionExpressions.push_back(expression);
        }
    }
    validateProjectionColumnNamesAreUnique(boundProjectionExpressions);
    return boundProjectionExpressions;
}

expression_vector QueryBinder::rewriteProjectionExpressions(const expression_vector& expressions) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (expression->dataType.typeID == NODE) {
            for (auto& property : rewriteNodeAsAllProperties(expression)) {
                result.push_back(property);
            }
        } else if (expression->dataType.typeID == REL) {
            for (auto& property : rewriteRelAsAllProperties(expression)) {
                result.push_back(property);
            }
        } else {
            result.push_back(expression);
        }
    }
    return result;
}

expression_vector QueryBinder::rewriteNodeAsAllProperties(
    const shared_ptr<Expression>& expression) {
    assert(expression->dataType.typeID == NODE);
    auto& node = (NodeExpression&)*expression;
    expression_vector result;
    for (auto& property : catalog.getAllNodeProperties(node.getLabel())) {
        result.emplace_back(make_shared<PropertyExpression>(
            property.dataType, property.name, property.id, expression));
    }
    return result;
}

expression_vector QueryBinder::rewriteRelAsAllProperties(const shared_ptr<Expression>& expression) {
    assert(expression->dataType.typeID == REL);
    auto& rel = (RelExpression&)*expression;
    expression_vector result;
    for (auto& property : catalog.getRelProperties(rel.getLabel())) {
        result.emplace_back(make_shared<PropertyExpression>(
            property.dataType, property.name, property.id, expression));
    }
    return result;
}

void QueryBinder::bindOrderBySkipLimitIfNecessary(
    BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody) {
    if (projectionBody.hasOrderByExpressions()) {
        // Cypher rule of ORDER BY expression scope: if projection contains aggregation, only
        // expressions in projection are available. Otherwise, expressions before projection are
        // also available
        if (boundProjectionBody.hasAggregationExpressions()) {
            variablesInScope.clear();
        }
        addExpressionsToScope(boundProjectionBody.getProjectionExpressions());
        boundProjectionBody.setOrderByExpressions(
            bindOrderByExpressions(projectionBody.getOrderByExpressions()),
            projectionBody.getSortOrders());
    }
    if (projectionBody.hasSkipExpression()) {
        boundProjectionBody.setSkipNumber(
            bindSkipLimitExpression(*projectionBody.getSkipExpression()));
    }
    if (projectionBody.hasLimitExpression()) {
        boundProjectionBody.setLimitNumber(
            bindSkipLimitExpression(*projectionBody.getLimitExpression()));
    }
}

expression_vector QueryBinder::bindOrderByExpressions(
    const vector<unique_ptr<ParsedExpression>>& orderByExpressions) {
    expression_vector boundOrderByExpressions;
    for (auto& expression : orderByExpressions) {
        boundOrderByExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    return boundOrderByExpressions;
}

uint64_t QueryBinder::bindSkipLimitExpression(const ParsedExpression& expression) {
    auto boundExpression = expressionBinder.bindExpression(expression);
    assert(boundExpression->expressionType == LITERAL_INT);
    auto skipOrLimitNumber = ((LiteralExpression&)(*boundExpression)).literal.val.int64Val;
    GF_ASSERT(skipOrLimitNumber >= 0);
    return skipOrLimitNumber;
}

void QueryBinder::addExpressionsToScope(const expression_vector& projectionExpressions) {
    for (auto& expression : projectionExpressions) {
        // In RETURN clause, if expression is not aliased, its input name will serve its alias.
        auto alias = expression->hasAlias() ? expression->getAlias() : expression->getRawName();
        variablesInScope.insert({alias, expression});
    }
}

shared_ptr<Expression> QueryBinder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    ExpressionBinder::implicitCastIfNecessary(whereExpression, BOOL);
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
        ExpressionBinder::validateExpectedDataType(*prevVariable, REL);
        throw BinderException("Bind relationship " + parsedName +
                              " to relationship with same name is not supported.");
    }
    auto relLabel = bindRelLabel(relPattern.getLabel());
    if (ANY_LABEL == relLabel) {
        throw BinderException(
            "Any-label is not supported. " + parsedName + " does not have a label.");
    }
    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.getDirection();
    validateNodeAndRelLabelIsConnected(
        catalog, relLabel, leftNode->getLabel(), isLeftNodeSrc ? FWD : BWD);
    validateNodeAndRelLabelIsConnected(
        catalog, relLabel, rightNode->getLabel(), isLeftNodeSrc ? BWD : FWD);
    auto srcNode = isLeftNodeSrc ? leftNode : rightNode;
    auto dstNode = isLeftNodeSrc ? rightNode : leftNode;
    // bind variable length
    auto lowerBound = min(TypeUtils::convertToUint32(relPattern.getLowerBound().c_str()),
        VAR_LENGTH_EXTEND_MAX_DEPTH);
    auto upperBound = min(TypeUtils::convertToUint32(relPattern.getUpperBound().c_str()),
        VAR_LENGTH_EXTEND_MAX_DEPTH);
    if (lowerBound == 0 || upperBound == 0) {
        throw BinderException("Lower and upper bound of a rel must be greater than 0.");
    }
    if (lowerBound > upperBound) {
        throw BinderException("Lower bound of rel " + parsedName + " is greater than upperBound.");
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
        ExpressionBinder::validateExpectedDataType(*prevVariable, NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto otherLabel = bindNodeLabel(nodePattern.getLabel());
        GF_ASSERT(queryNode->getLabel() != ANY_LABEL);
        if (otherLabel != ANY_LABEL && queryNode->getLabel() != otherLabel) {
            throw BinderException(
                "Multi-label is not supported. Node " + parsedName + " is given multiple labels.");
        }
    } else { // create new node
        auto nodeLabel = bindNodeLabel(nodePattern.getLabel());
        queryNode = make_shared<NodeExpression>(getUniqueExpressionName(parsedName), nodeLabel);
        queryNode->setAlias(parsedName);
        queryNode->setRawName(parsedName);
        if (ANY_LABEL == nodeLabel) {
            throw BinderException(
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
        throw BinderException("Rel label " + parsed_label + " does not exist.");
    }
    return catalog.getRelLabelFromName(parsed_label);
}

label_t QueryBinder::bindNodeLabel(const string& parsed_label) {
    if (parsed_label.empty()) {
        return ANY_LABEL;
    }
    if (!catalog.containNodeLabel(parsed_label)) {
        throw BinderException("Node label " + parsed_label + " does not exist.");
    }
    return catalog.getNodeLabelFromName(parsed_label);
}

void QueryBinder::validateFirstMatchIsNotOptional(const SingleQuery& singleQuery) {
    if (singleQuery.isFirstMatchOptional()) {
        throw BinderException("First match clause cannot be optional match.");
    }
}

void QueryBinder::validateNodeAndRelLabelIsConnected(
    const Catalog& catalog_, label_t relLabel, label_t nodeLabel, RelDirection direction) {
    assert(relLabel != ANY_LABEL);
    assert(nodeLabel != ANY_LABEL);
    auto connectedRelLabels = catalog_.getRelLabelsForNodeLabelDirection(nodeLabel, direction);
    for (auto& connectedRelLabel : connectedRelLabels) {
        if (relLabel == connectedRelLabel) {
            return;
        }
    }
    throw BinderException("Node label " + catalog_.getNodeLabelName(nodeLabel) +
                          " doesn't connect to rel label " + catalog_.getRelLabelName(relLabel) +
                          ".");
}

void QueryBinder::validateProjectionColumnNamesAreUnique(const expression_vector& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        if (existColumnNames.contains(expression->getRawName())) {
            throw BinderException("Multiple result column with the same name " +
                                  expression->getRawName() + " are not supported.");
        }
        existColumnNames.insert(expression->getRawName());
    }
}

void QueryBinder::validateProjectionColumnsInWithClauseAreAliased(
    const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (!expression->hasAlias()) {
            throw BinderException("Expression in WITH must be aliased (use AS).");
        }
    }
}

void QueryBinder::validateOrderByFollowedBySkipOrLimitInWithClause(
    const BoundProjectionBody& boundProjectionBody) {
    auto hasSkipOrLimit = boundProjectionBody.hasSkip() || boundProjectionBody.hasLimit();
    if (boundProjectionBody.hasOrderByExpressions() && !hasSkipOrLimit) {
        throw BinderException("In WITH clause, ORDER BY must be followed by SKIP or LIMIT.");
    }
}

void QueryBinder::validateQueryGraphIsConnected(const QueryGraph& queryGraph,
    const unordered_map<string, shared_ptr<Expression>>& prevVariablesInScope) {
    auto visited = unordered_set<string>();
    for (auto& [name, variable] : prevVariablesInScope) {
        if (NODE == variable->dataType.typeID) {
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
    throw BinderException("Disconnect query graph is not supported.");
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
            throw BinderException("The number of columns to union/union all must be the same.");
        }
        // Check whether the dataTypes in union expressions are exactly the same in each single
        // query.
        for (auto j = 0u; j < expressionsToProject.size(); j++) {
            ExpressionBinder::validateExpectedDataType(
                *expressionsToProjectToCheck[j], expressionsToProject[j]->dataType.typeID);
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
        throw BinderException("Union and union all can't be used together.");
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
