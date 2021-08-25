#include "src/binder/include/query_binder.h"

#include <fstream>

#include "src/binder/include/bound_statements/bound_load_csv_statement.h"
#include "src/binder/include/bound_statements/bound_match_statement.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/common/include/assert.h"
#include "src/common/include/csv_reader/csv_reader.h"

namespace graphflow {
namespace binder {

const char DEFAULT_LOAD_CSV_TOKEN_SEPARATOR = ',';

// TODO: GraphLoader has a similar logic of parsing header. Refactor a function under common.
static vector<pair<string, DataType>> parseCSVHeader(const string& headerLine, char tokenSeparator);
static vector<unique_ptr<BoundReadingStatement>> mergeAllMatchStatements(
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements);

unique_ptr<BoundSingleQuery> QueryBinder::bind(const SingleQuery& singleQuery) {
    auto boundSingleQuery = bindSingleQuery(singleQuery);
    optimizeReadingStatements(*boundSingleQuery);
    return boundSingleQuery;
}

void QueryBinder::optimizeReadingStatements(BoundSingleQuery& boundSingleQuery) {
    for (auto& boundQueryPart : boundSingleQuery.boundQueryParts) {
        boundQueryPart->boundReadingStatements =
            mergeAllMatchStatements(move(boundQueryPart->boundReadingStatements));
    }
    boundSingleQuery.boundReadingStatements =
        mergeAllMatchStatements(move(boundSingleQuery.boundReadingStatements));
}

unique_ptr<BoundSingleQuery> QueryBinder::bindSingleQuery(const SingleQuery& singleQuery) {
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto& queryPart : singleQuery.queryParts) {
        boundSingleQuery->boundQueryParts.push_back(bindQueryPart(*queryPart));
    }
    for (auto& readingStatement : singleQuery.readingStatements) {
        boundSingleQuery->boundReadingStatements.push_back(bindReadingStatement(*readingStatement));
    }
    boundSingleQuery->boundReturnStatement = bindReturnStatement(*singleQuery.returnStatement);
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> QueryBinder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto& readingStatement : queryPart.readingStatements) {
        boundQueryPart->boundReadingStatements.push_back(bindReadingStatement(*readingStatement));
    }
    boundQueryPart->boundWithStatement = bindWithStatement(*queryPart.withStatement);
    return boundQueryPart;
}

unique_ptr<BoundReadingStatement> QueryBinder::bindReadingStatement(
    const ReadingStatement& readingStatement) {
    if (MATCH_STATEMENT == readingStatement.statementType) {
        return bindMatchStatement((MatchStatement&)readingStatement);
    }
    return bindLoadCSVStatement((LoadCSVStatement&)readingStatement);
}

unique_ptr<BoundReadingStatement> QueryBinder::bindLoadCSVStatement(
    const LoadCSVStatement& loadCSVStatement) {
    auto tokenSeparator = DEFAULT_LOAD_CSV_TOKEN_SEPARATOR;
    if (!loadCSVStatement.fieldTerminator.empty()) {
        if (loadCSVStatement.fieldTerminator.size() > 1) {
            throw invalid_argument("FIELDTERMINATOR has data type STRING. CHAR was expected.");
        }
        tokenSeparator = loadCSVStatement.fieldTerminator.at(0);
    }
    auto boundInputExpression = expressionBinder.bindExpression(*loadCSVStatement.inputExpression);
    if (LITERAL_STRING != boundInputExpression->expressionType) {
        throw invalid_argument("LOAD CSV FROM " +
                               expressionTypeToString(boundInputExpression->expressionType) +
                               " is not supported. STRING was expected.");
    }
    auto filePath = static_pointer_cast<LiteralExpression>(boundInputExpression)->literal.strVal;
    auto fileStream = ifstream(filePath, ios_base::in);
    if (!fileStream.is_open()) {
        throw invalid_argument("Cannot open file at " + filePath + ".");
    }
    string headerLine;
    getline(fileStream, headerLine);
    auto headerInfo = parseCSVHeader(headerLine, tokenSeparator);
    validateCSVHeaderColumnNamesAreUnique(headerInfo);
    fileStream.seekg(0, ios_base::end);
    /**
     * In order to avoid creating LIST datatype or expression, we directly create
     * csvColumnExpression with type CSV_LINE_EXTRACT for each column e.g. csvLine[0], csvLine[1]
     * ... One consequence is we won't allow user to refer csvLine later. Instead, we force user to
     * write csvLine[i]. By doing so, we don't need to implement evaluator for list.
     */
    auto lineVariableName = loadCSVStatement.lineVariableName;
    auto csvColumnVariables = vector<shared_ptr<VariableExpression>>();
    for (auto i = 0u; i < headerInfo.size(); ++i) {
        auto csvColumnVariableAliasName = lineVariableName + "[" + to_string(i) + "]";
        auto csvColumnVariable = make_shared<VariableExpression>(CSV_LINE_EXTRACT,
            headerInfo[i].second, getUniqueVariableName(csvColumnVariableAliasName));
        csvColumnVariable->rawExpression = csvColumnVariableAliasName;
        variablesInScope.insert({csvColumnVariableAliasName, csvColumnVariable});
        csvColumnVariables.push_back(csvColumnVariable);
    }
    return make_unique<BoundLoadCSVStatement>(filePath, tokenSeparator, move(csvColumnVariables));
}

unique_ptr<BoundReadingStatement> QueryBinder::bindMatchStatement(
    const MatchStatement& matchStatement) {
    auto prevVariablesInScope = variablesInScope;
    auto queryGraph = bindQueryGraph(matchStatement.graphPattern);
    validateQueryGraphIsConnected(*queryGraph, prevVariablesInScope);
    auto boundMatchStatement = make_unique<BoundMatchStatement>(move(queryGraph));
    if (matchStatement.whereClause) {
        boundMatchStatement->whereExpression = bindWhereExpression(*matchStatement.whereClause);
    }
    return boundMatchStatement;
}

unique_ptr<BoundWithStatement> QueryBinder::bindWithStatement(const WithStatement& withStatement) {
    auto boundWithStatement = make_unique<BoundWithStatement>(
        bindProjectionBody(*withStatement.getProjectionBody(), true));
    if (withStatement.hasWhereExpression()) {
        boundWithStatement->setWhereExpression(
            bindWhereExpression(*withStatement.getWhereExpression()));
    }
    return boundWithStatement;
}

unique_ptr<BoundReturnStatement> QueryBinder::bindReturnStatement(
    const ReturnStatement& returnStatement) {
    auto boundReturnStatement = make_unique<BoundReturnStatement>(
        bindProjectionBody(*returnStatement.getProjectionBody(), false));
    return boundReturnStatement;
}

unique_ptr<BoundProjectionBody> QueryBinder::bindProjectionBody(
    const ProjectionBody& projectionBody, bool updateVariablesInScope) {
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        bindProjectExpressions(projectionBody.getProjectionExpressions(),
            projectionBody.isProjectStar(), updateVariablesInScope));
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

shared_ptr<Expression> QueryBinder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    if (BOOL != whereExpression->dataType) {
        throw invalid_argument("Type mismatch: " + whereExpression->rawExpression + " returns " +
                               TypeUtils::dataTypeToString(whereExpression->dataType) +
                               " expected Boolean.");
    }
    return whereExpression;
}

vector<shared_ptr<Expression>> QueryBinder::bindProjectExpressions(
    const vector<unique_ptr<ParsedExpression>>& parsedExpressions, bool projectStar,
    bool updateVariablesInScope) {
    auto expressions = vector<shared_ptr<Expression>>();
    unordered_map<string, shared_ptr<Expression>> newVariablesInScope;
    if (projectStar) {
        if (variablesInScope.empty()) {
            throw invalid_argument(
                "RETURN or WITH * is not allowed when there are no variables in scope.");
        }
        for (auto& [name, expression] : variablesInScope) {
            if (updateVariablesInScope) {
                newVariablesInScope.insert({name, expression});
            }
            expressions.push_back(expression);
        }
    }
    for (auto& parsedExpression : parsedExpressions) {
        auto expression = expressionBinder.bindExpression(*parsedExpression);
        if (updateVariablesInScope) {
            if (expression->expressionType == ALIAS || expression->expressionType == VARIABLE) {
                newVariablesInScope.insert({expression->getExternalName(), expression});
            } else {
                throw invalid_argument("Expression in WITH multi be aliased (use AS).");
            }
        }
        expressions.push_back(expression);
    }
    variablesInScope = newVariablesInScope;
    validateProjectionColumnNamesAreUnique(expressions);
    validateOnlyFunctionIsCountStar(expressions);
    return expressions;
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
        getUniqueVariableName(parsedName), relLabel, srcNode, dstNode, lowerBound, upperBound);
    queryRel->rawExpression = parsedName;
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
        while (ALIAS == prevVariable->expressionType) {
            prevVariable = prevVariable->children[0];
        }
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
        queryNode = make_shared<NodeExpression>(getUniqueVariableName(parsedName), nodeLabel);
        queryNode->rawExpression = parsedName;
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
        if (existColumnNames.contains(expression->getExternalName())) {
            throw invalid_argument("Multiple result column with the same name " +
                                   expression->getExternalName() + " are not supported.");
        }
        existColumnNames.insert(expression->getExternalName());
    }
}

void QueryBinder::validateOnlyFunctionIsCountStar(
    const vector<shared_ptr<Expression>>& expressions) {
    for (auto& expression : expressions) {
        if (COUNT_STAR_FUNC == expression->expressionType && 1 != expressions.size()) {
            throw invalid_argument("The only function in the return clause should be COUNT(*).");
        }
    }
}

void QueryBinder::validateQueryGraphIsConnected(const QueryGraph& queryGraph,
    unordered_map<string, shared_ptr<Expression>> prevVariablesInScope) {
    auto visited = unordered_set<string>();
    for (auto& [name, variable] : prevVariablesInScope) {
        if (NODE == variable->dataType) {
            visited.insert(variable->getInternalName());
        }
    }
    if (visited.empty()) {
        visited.insert(queryGraph.queryNodes[0]->getInternalName());
    }
    auto target = visited;
    for (auto& queryNode : queryGraph.queryNodes) {
        target.insert(queryNode->getInternalName());
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

void QueryBinder::validateCSVHeaderColumnNamesAreUnique(
    const vector<pair<string, DataType>>& headerInfo) {
    auto propertyNames = unordered_set<string>();
    for (auto& [propertyName, dataType] : headerInfo) {
        if (propertyNames.contains(propertyName)) {
            throw invalid_argument("Multiple property columns with the same name " + propertyName +
                                   " is not supported.");
        }
        propertyNames.insert(propertyName);
    }
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

string QueryBinder::getUniqueVariableName(const string& name) {
    return "_" + to_string(lastVariableId++) + "_" + name;
}

unordered_map<string, shared_ptr<Expression>> QueryBinder::enterSubquery() {
    return variablesInScope;
}

void QueryBinder::exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope) {
    variablesInScope = move(prevVariablesInScope);
}

vector<pair<string, DataType>> parseCSVHeader(const string& headerLine, char tokenSeparator) {
    auto headerInfo = vector<pair<string, DataType>>();
    for (auto& entry : StringUtils::split(headerLine, string(1, tokenSeparator))) {
        auto propertyDataType = StringUtils::split(entry, PROPERTY_DATATYPE_SEPARATOR);
        if (propertyDataType.size() == 1) {
            throw invalid_argument("Missing colon separated dataType in " + entry + ".");
        } else if (propertyDataType.size() > 2) {
            throw invalid_argument("Multiple dataType in " + entry + " is not supported.");
        }
        headerInfo.emplace_back(
            make_pair(propertyDataType[0], TypeUtils::getDataType(propertyDataType[1])));
    }
    return headerInfo;
}

// Merge all match statements as one given a list of reading statement.
// This is still valid in the case of MATCH LOAD CSV MATCH. Because our LOAD CSV reads header during
// binding, it cannot depend on runtime output of previous match. So the order of MATCH and LOAD CSV
// doesn't matter (similar to Cartesian Product).
vector<unique_ptr<BoundReadingStatement>> mergeAllMatchStatements(
    vector<unique_ptr<BoundReadingStatement>> boundReadingStatements) {
    auto result = vector<unique_ptr<BoundReadingStatement>>();
    auto mergedMatchStatement = make_unique<BoundMatchStatement>(make_unique<QueryGraph>());
    for (auto& boundReadingStatement : boundReadingStatements) {
        if (LOAD_CSV_STATEMENT == boundReadingStatement->statementType) {
            result.push_back(move(boundReadingStatement));
        } else {
            mergedMatchStatement->merge((BoundMatchStatement&)*boundReadingStatement);
        }
    }
    result.push_back(move(mergedMatchStatement));
    return result;
}

} // namespace binder
} // namespace graphflow
