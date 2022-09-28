#include "src/binder/include/binder.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/common/include/type_utils.h"
#include "src/parser/query/updating_clause/include/create_clause.h"
#include "src/parser/query/updating_clause/include/delete_clause.h"
#include "src/parser/query/updating_clause/include/set_clause.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundStatement> Binder::bind(const Statement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        return bindQuery((const RegularQuery&)statement);
    }
    case StatementType::CREATE_NODE_CLAUSE: {
        return bindCreateNodeClause((const CreateNodeClause&)statement);
    }
    case StatementType::CREATE_REL_CLAUSE: {
        return bindCreateRelClause((const CreateRelClause&)statement);
    }
    case StatementType::COPY_CSV: {
        return bindCopyCSV((const CopyCSV&)statement);
    }
    case StatementType::DROP_TABLE: {
        return bindDropTable((const DropTable&)statement);
    }
    default:
        assert(false);
    }
}

unique_ptr<BoundRegularQuery> Binder::bindQuery(const RegularQuery& regularQuery) {
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
        auto normalizedSingleQuery = QueryNormalizer::normalizeQuery(*boundSingleQuery);
        validateReadNotFollowUpdate(*normalizedSingleQuery);
        boundRegularQuery->addSingleQuery(move(normalizedSingleQuery));
    }
    validateIsAllUnionOrUnionAll(*boundRegularQuery);
    return boundRegularQuery;
}

unique_ptr<BoundCreateNodeClause> Binder::bindCreateNodeClause(
    const CreateNodeClause& createNodeClause) {
    auto tableName = createNodeClause.getTableName();
    if (catalog.getReadOnlyVersion()->containNodeTable(tableName)) {
        throw BinderException("Node " + tableName + " already exists.");
    }
    auto boundPropertyNameDataTypes =
        bindPropertyNameDataTypes(createNodeClause.getPropertyNameDataTypes());
    auto primaryKeyIdx = bindPrimaryKey(
        createNodeClause.getPrimaryKey(), createNodeClause.getPropertyNameDataTypes());
    return make_unique<BoundCreateNodeClause>(
        tableName, move(boundPropertyNameDataTypes), primaryKeyIdx);
}

unique_ptr<BoundCreateRelClause> Binder::bindCreateRelClause(
    const CreateRelClause& createRelClause) {
    auto tableName = createRelClause.getTableName();
    if (catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Rel " + tableName + " already exists.");
    }
    auto propertyNameDataTypes =
        bindPropertyNameDataTypes(createRelClause.getPropertyNameDataTypes());
    auto relMultiplicity = getRelMultiplicityFromString(createRelClause.getRelMultiplicity());
    auto srcDstTableIDs = bindRelConnections(createRelClause.getRelConnection());
    return make_unique<BoundCreateRelClause>(
        tableName, move(propertyNameDataTypes), relMultiplicity, move(srcDstTableIDs));
}

unique_ptr<BoundCopyCSV> Binder::bindCopyCSV(const CopyCSV& copyCSV) {
    auto tableName = copyCSV.getTableName();
    validateTableExist(tableName);
    auto isNodeTable = catalog.getReadOnlyVersion()->containNodeTable(tableName);
    auto tableID = isNodeTable ? catalog.getReadOnlyVersion()->getNodeTableIDFromName(tableName) :
                                 catalog.getReadOnlyVersion()->getRelTableIDFromName(tableName);
    return make_unique<BoundCopyCSV>(
        CSVDescription(copyCSV.getCSVFileName(), bindParsingOptions(copyCSV.getParsingOptions())),
        TableSchema(tableName, tableID, isNodeTable));
}

unique_ptr<BoundDropTable> Binder::bindDropTable(const DropTable& dropTable) {
    auto tableName = dropTable.getTableName();
    validateTableExist(tableName);
    auto catalogContentForReadOnlyVersion = catalog.getReadOnlyVersion();
    auto isNodeTable = catalogContentForReadOnlyVersion->containNodeTable(tableName);
    auto tableID = isNodeTable ?
                       catalogContentForReadOnlyVersion->getNodeTableIDFromName(tableName) :
                       catalogContentForReadOnlyVersion->getRelTableIDFromName(tableName);
    if (isNodeTable) {
        validateNodeTableHasNoEdge(tableID);
    }
    return make_unique<BoundDropTable>(
        isNodeTable ? (TableSchema*)catalog.getReadOnlyVersion()->getNodeTableSchema(tableID) :
                      (TableSchema*)catalog.getReadOnlyVersion()->getRelTableSchema(tableID));
}

SrcDstTableIDs Binder::bindRelConnections(RelConnection relConnections) const {
    unordered_set<table_id_t> srcTableIDs, dstTableIDs;
    for (auto& srcTableName : relConnections.srcTableNames) {
        for (auto& dstTableName : relConnections.dstTableNames) {
            srcTableIDs.insert(bindNodeTableName(srcTableName));
            dstTableIDs.insert(bindNodeTableName(dstTableName));
        }
    }
    return SrcDstTableIDs(move(srcTableIDs), move(dstTableIDs));
}

CSVReaderConfig Binder::bindParsingOptions(
    const unordered_map<string, unique_ptr<ParsedExpression>>* parsingOptions) {
    CSVReaderConfig csvReaderConfig;
    for (auto& parsingOption : *parsingOptions) {
        auto copyOptionName = parsingOption.first;
        bool isValidStringParsingOption = validateStringParsingOptionName(copyOptionName);
        auto copyOptionExpression = parsingOption.second.get();
        auto boundCopyOptionExpression = expressionBinder.bindExpression(*copyOptionExpression);
        assert(boundCopyOptionExpression->expressionType = LITERAL);

        if (copyOptionName == "HEADER") {
            if (boundCopyOptionExpression->dataType.typeID != BOOL) {
                throw "The value type of parsing csv option " + copyOptionName +
                    " must be boolean.";
            }
            csvReaderConfig.hasHeader =
                ((LiteralExpression&)(*boundCopyOptionExpression)).literal->val.booleanVal;
        } else if (boundCopyOptionExpression->dataType.typeID == STRING &&
                   isValidStringParsingOption) {
            if (boundCopyOptionExpression->dataType.typeID != STRING) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be string.");
            }
            auto copyOptionValue =
                ((LiteralExpression&)(*boundCopyOptionExpression)).literal->strVal;
            bindStringParsingOptions(csvReaderConfig, copyOptionName, copyOptionValue);
        } else {
            throw BinderException("Unrecognized parsing csv option: " + copyOptionName + ".");
        }
    }
    return csvReaderConfig;
}

void Binder::bindStringParsingOptions(
    CSVReaderConfig& csvReaderConfig, const string& copyOptionName, string& copyOptionValue) {
    auto parsingOptionValue = bindParsingOptionValue(copyOptionValue);
    if (copyOptionName == "ESCAPE") {
        csvReaderConfig.escapeChar = parsingOptionValue;
    } else if (copyOptionName == "DELIM") {
        csvReaderConfig.tokenSeparator = parsingOptionValue;
    } else if (copyOptionName == "QUOTE") {
        csvReaderConfig.quoteChar = parsingOptionValue;
    } else if (copyOptionName == "LIST_BEGIN") {
        csvReaderConfig.listBeginChar = parsingOptionValue;
    } else if (copyOptionName == "LIST_END") {
        csvReaderConfig.listEndChar = parsingOptionValue;
    }
}

char Binder::bindParsingOptionValue(string parsingOptionValue) {
    if (parsingOptionValue.length() > 2 ||
        (parsingOptionValue.length() == 2 && parsingOptionValue[0] != '\\')) {
        throw BinderException("Copy csv option value can only be a single character with an "
                              "optional escape character.");
    }
    return parsingOptionValue[parsingOptionValue.length() - 1];
}

unique_ptr<BoundSingleQuery> Binder::bindSingleQuery(const SingleQuery& singleQuery) {
    validateFirstMatchIsNotOptional(singleQuery);
    auto boundSingleQuery = make_unique<BoundSingleQuery>();
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        boundSingleQuery->addQueryPart(bindQueryPart(*singleQuery.getQueryPart(i)));
    }
    for (auto i = 0u; i < singleQuery.getNumReadingClauses(); i++) {
        boundSingleQuery->addReadingClause(bindReadingClause(*singleQuery.getReadingClause(i)));
    }
    for (auto i = 0u; i < singleQuery.getNumUpdatingClauses(); ++i) {
        boundSingleQuery->addUpdatingClause(bindUpdatingClause(*singleQuery.getUpdatingClause(i)));
    }
    if (singleQuery.hasReturnClause()) {
        boundSingleQuery->setReturnClause(bindReturnClause(*singleQuery.getReturnClause()));
    }
    return boundSingleQuery;
}

unique_ptr<BoundQueryPart> Binder::bindQueryPart(const QueryPart& queryPart) {
    auto boundQueryPart = make_unique<BoundQueryPart>();
    for (auto i = 0u; i < queryPart.getNumReadingClauses(); i++) {
        boundQueryPart->addReadingClause(bindReadingClause(*queryPart.getReadingClause(i)));
    }
    for (auto i = 0u; i < queryPart.getNumUpdatingClauses(); ++i) {
        boundQueryPart->addUpdatingClause(bindUpdatingClause(*queryPart.getUpdatingClause(i)));
    }
    boundQueryPart->setWithClause(bindWithClause(*queryPart.getWithClause()));
    return boundQueryPart;
}

unique_ptr<BoundReadingClause> Binder::bindReadingClause(const ReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        return bindMatchClause((MatchClause&)readingClause);
    } break;
    case ClauseType::UNWIND: {
        return bindUnwindClause((UnwindClause&)readingClause);
    } break;
    default:
        assert(false);
    }
}

unique_ptr<BoundMatchClause> Binder::bindMatchClause(const MatchClause& matchClause) {
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

unique_ptr<BoundUnwindClause> Binder::bindUnwindClause(const UnwindClause& unwindClause) {
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    variablesInScope.insert({unwindClause.getAlias(), boundExpression});
    return make_unique<BoundUnwindClause>(move(boundExpression), unwindClause.getAlias());
}

unique_ptr<BoundUpdatingClause> Binder::bindUpdatingClause(const UpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        return bindCreateClause(updatingClause);
    }
    case ClauseType::SET: {
        return bindSetClause(updatingClause);
    }
    case ClauseType::DELETE: {
        return bindDeleteClause(updatingClause);
    }
    default:
        assert(false);
    }
}

unique_ptr<BoundUpdatingClause> Binder::bindCreateClause(const UpdatingClause& updatingClause) {
    auto& createClause = (CreateClause&)updatingClause;
    auto boundCreateClause = make_unique<BoundCreateClause>();
    for (auto i = 0u; i < createClause.getNumNodePatterns(); ++i) {
        auto nodePattern = createClause.getNodePattern(i);
        auto node = createQueryNode(*nodePattern);
        auto nodeUpdateInfo = make_unique<NodeUpdateInfo>(node);
        for (auto j = 0u; j < nodePattern->getNumProperties(); ++j) {
            auto [propertyName, target] = nodePattern->getProperty(j);
            auto boundProperty = expressionBinder.bindNodePropertyExpression(node, propertyName);
            auto boundTarget = expressionBinder.bindExpression(*target);
            boundTarget = ExpressionBinder::implicitCastIfNecessary(
                boundTarget, boundProperty->dataType.typeID);
            nodeUpdateInfo->addPropertyUpdateInfo(
                make_unique<PropertyUpdateInfo>(move(boundProperty), move(boundTarget)));
        }
        validateNodeCreateHasPrimaryKeyInput(*nodeUpdateInfo,
            catalog.getReadOnlyVersion()->getNodePrimaryKeyProperty(node->getTableID()));
        boundCreateClause->addNodeUpdateInfo(move(nodeUpdateInfo));
    }
    return boundCreateClause;
}

unique_ptr<BoundUpdatingClause> Binder::bindSetClause(const UpdatingClause& updatingClause) {
    auto& setClause = (SetClause&)updatingClause;
    auto boundSetClause = make_unique<BoundSetClause>();
    for (auto i = 0u; i < setClause.getNumSetItems(); ++i) {
        auto setItem = setClause.getSetItem(i);
        auto boundProperty = expressionBinder.bindExpression(*setItem->origin);
        auto boundTarget = expressionBinder.bindExpression(*setItem->target);
        boundTarget =
            ExpressionBinder::implicitCastIfNecessary(boundTarget, boundProperty->dataType.typeID);
        boundSetClause->addUpdateInfo(
            make_unique<PropertyUpdateInfo>(move(boundProperty), move(boundTarget)));
    }
    return boundSetClause;
}

unique_ptr<BoundUpdatingClause> Binder::bindDeleteClause(const UpdatingClause& updatingClause) {
    auto& deleteClause = (DeleteClause&)updatingClause;
    auto boundDeleteClause = make_unique<BoundDeleteClause>();
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto boundExpression = expressionBinder.bindExpression(*deleteClause.getExpression(i));
        if (boundExpression->dataType.typeID != NODE) {
            throw BinderException("Delete " +
                                  expressionTypeToString(boundExpression->expressionType) +
                                  " is not supported.");
        }
        boundDeleteClause->addExpression(move(boundExpression));
    }
    return boundDeleteClause;
}

unique_ptr<BoundWithClause> Binder::bindWithClause(const WithClause& withClause) {
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

unique_ptr<BoundReturnClause> Binder::bindReturnClause(const ReturnClause& returnClause) {
    auto projectionBody = returnClause.getProjectionBody();
    auto boundProjectionExpressions = rewriteProjectionExpressions(bindProjectionExpressions(
        projectionBody->getProjectionExpressions(), projectionBody->getContainsStar()));
    auto boundProjectionBody = make_unique<BoundProjectionBody>(
        projectionBody->getIsDistinct(), move(boundProjectionExpressions));
    bindOrderBySkipLimitIfNecessary(*boundProjectionBody, *projectionBody);
    return make_unique<BoundReturnClause>(move(boundProjectionBody));
}

expression_vector Binder::bindProjectionExpressions(
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

expression_vector Binder::rewriteProjectionExpressions(const expression_vector& expressions) {
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

expression_vector Binder::rewriteNodeAsAllProperties(const shared_ptr<Expression>& expression) {
    auto& node = (NodeExpression&)*expression;
    expression_vector result;
    for (auto& property : catalog.getReadOnlyVersion()->getAllNodeProperties(node.getTableID())) {
        auto propertyExpression = make_shared<PropertyExpression>(
            property.dataType, property.name, property.propertyID, expression);
        propertyExpression->setRawName(expression->getRawName() + "." + property.name);
        result.emplace_back(propertyExpression);
    }
    return result;
}

expression_vector Binder::rewriteRelAsAllProperties(const shared_ptr<Expression>& expression) {
    auto& rel = (RelExpression&)*expression;
    expression_vector result;
    for (auto& property : catalog.getReadOnlyVersion()->getRelProperties(rel.getTableID())) {
        auto propertyExpression = make_shared<PropertyExpression>(
            property.dataType, property.name, property.propertyID, expression);
        propertyExpression->setRawName(expression->getRawName() + "." + property.name);
        result.emplace_back(propertyExpression);
    }
    return result;
}

void Binder::bindOrderBySkipLimitIfNecessary(
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

expression_vector Binder::bindOrderByExpressions(
    const vector<unique_ptr<ParsedExpression>>& orderByExpressions) {
    expression_vector boundOrderByExpressions;
    for (auto& expression : orderByExpressions) {
        boundOrderByExpressions.push_back(expressionBinder.bindExpression(*expression));
    }
    return boundOrderByExpressions;
}

uint64_t Binder::bindSkipLimitExpression(const ParsedExpression& expression) {
    auto boundExpression = expressionBinder.bindExpression(expression);
    auto skipOrLimitNumber = ((LiteralExpression&)(*boundExpression)).literal->val.int64Val;
    GF_ASSERT(skipOrLimitNumber >= 0);
    return skipOrLimitNumber;
}

void Binder::addExpressionsToScope(const expression_vector& projectionExpressions) {
    for (auto& expression : projectionExpressions) {
        // In RETURN clause, if expression is not aliased, its input name will serve its alias.
        auto alias = expression->hasAlias() ? expression->getAlias() : expression->getRawName();
        variablesInScope.insert({alias, expression});
    }
}

shared_ptr<Expression> Binder::bindWhereExpression(const ParsedExpression& parsedExpression) {
    auto whereExpression = expressionBinder.bindExpression(parsedExpression);
    ExpressionBinder::implicitCastIfNecessary(whereExpression, BOOL);
    return whereExpression;
}

unique_ptr<QueryGraph> Binder::bindQueryGraph(
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

void Binder::bindQueryRel(const RelPattern& relPattern, const shared_ptr<NodeExpression>& leftNode,
    const shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph) {
    auto parsedName = relPattern.getVariableName();
    if (variablesInScope.contains(parsedName)) {
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, REL);
        throw BinderException("Bind relationship " + parsedName +
                              " to relationship with same name is not supported.");
    }
    auto tableID = bindRelTable(relPattern.getTableName());
    if (ANY_TABLE_ID == tableID) {
        throw BinderException(
            "Any-table is not supported. " + parsedName + " does not have a table.");
    }
    // bind node to rel
    auto isLeftNodeSrc = RIGHT == relPattern.getDirection();
    validateNodeAndRelTableIsConnected(
        catalog, tableID, leftNode->getTableID(), isLeftNodeSrc ? FWD : BWD);
    validateNodeAndRelTableIsConnected(
        catalog, tableID, rightNode->getTableID(), isLeftNodeSrc ? BWD : FWD);
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
        getUniqueExpressionName(parsedName), tableID, srcNode, dstNode, lowerBound, upperBound);
    queryRel->setAlias(parsedName);
    queryRel->setRawName(parsedName);
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryRel});
    }
    queryGraph.addQueryRel(queryRel);
}

shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph) {
    if (nodePattern.getNumProperties() > 0) {
        // E.g. MATCH (a:person {p1:v1}) is not supported.
        throw BinderException("Node pattern with properties in MATCH clause is not supported.");
    }
    auto parsedName = nodePattern.getVariableName();
    shared_ptr<NodeExpression> queryNode;
    if (variablesInScope.contains(parsedName)) { // bind to node in scope
        auto prevVariable = variablesInScope.at(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        auto otherTableID = bindNodeTableName(nodePattern.getTableName());
        GF_ASSERT(queryNode->getTableID() != ANY_TABLE_ID);
        if (otherTableID != ANY_TABLE_ID && queryNode->getTableID() != otherTableID) {
            throw BinderException(
                "Multi-table is not supported. Node " + parsedName + " is given multiple tables.");
        }
    } else {
        queryNode = createQueryNode(nodePattern);
    }
    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

shared_ptr<NodeExpression> Binder::createQueryNode(const NodePattern& nodePattern) {
    auto parsedName = nodePattern.getVariableName();
    auto tableID = bindNodeTableName(nodePattern.getTableName());
    auto queryNode = make_shared<NodeExpression>(getUniqueExpressionName(parsedName), tableID);
    queryNode->setAlias(parsedName);
    queryNode->setRawName(parsedName);
    if (ANY_TABLE_ID == tableID) {
        throw BinderException(
            "Any-table is not supported. " + parsedName + " does not have a table.");
    }
    if (!parsedName.empty()) {
        variablesInScope.insert({parsedName, queryNode});
    }
    return queryNode;
}

table_id_t Binder::bindRelTable(const string& tableName) const {
    if (tableName.empty()) {
        return ANY_TABLE_ID;
    }
    if (!catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Rel table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getRelTableIDFromName(tableName);
}

table_id_t Binder::bindNodeTableName(const string& tableName) const {
    if (tableName.empty()) {
        return ANY_TABLE_ID;
    }
    if (!catalog.getReadOnlyVersion()->containNodeTable(tableName)) {
        throw BinderException("Node table " + tableName + " does not exist.");
    }
    return catalog.getReadOnlyVersion()->getNodeTableIDFromName(tableName);
}

uint32_t Binder::bindPrimaryKey(
    string primaryKey, vector<pair<string, string>> propertyNameDataTypes) {
    auto primaryKeyIdx = 0u;
    for (auto i = 0u; i < propertyNameDataTypes.size(); i++) {
        if (propertyNameDataTypes[i].first == primaryKey) {
            primaryKeyIdx = i;
        }
    }
    validatePrimaryKey(primaryKeyIdx, move(propertyNameDataTypes));
    return primaryKeyIdx;
}

vector<PropertyNameDataType> Binder::bindPropertyNameDataTypes(
    vector<pair<string, string>> propertyNameDataTypes) {
    vector<PropertyNameDataType> boundPropertyNameDataTypes;
    for (auto& propertyNameDataType : propertyNameDataTypes) {
        StringUtils::toUpper(propertyNameDataType.second);
        boundPropertyNameDataTypes.emplace_back(
            propertyNameDataType.first, Types::dataTypeFromString(propertyNameDataType.second));
    }
    return boundPropertyNameDataTypes;
}

void Binder::validateFirstMatchIsNotOptional(const SingleQuery& singleQuery) {
    if (singleQuery.isFirstReadingClauseOptionalMatch()) {
        throw BinderException("First match clause cannot be optional match.");
    }
}

void Binder::validateNodeAndRelTableIsConnected(const Catalog& catalog_, table_id_t relTableID,
    table_id_t nodeTableID, RelDirection direction) {
    assert(relTableID != ANY_TABLE_ID);
    assert(nodeTableID != ANY_TABLE_ID);
    auto connectedRelTableIDs =
        catalog_.getReadOnlyVersion()->getRelTableIDsForNodeTableDirection(nodeTableID, direction);
    for (auto& connectedRelTableID : connectedRelTableIDs) {
        if (relTableID == connectedRelTableID) {
            return;
        }
    }
    throw BinderException("Node table " +
                          catalog_.getReadOnlyVersion()->getNodeTableName(nodeTableID) +
                          " doesn't connect to rel table " +
                          catalog_.getReadOnlyVersion()->getRelTableName(relTableID) + ".");
}

void Binder::validateProjectionColumnNamesAreUnique(const expression_vector& expressions) {
    auto existColumnNames = unordered_set<string>();
    for (auto& expression : expressions) {
        if (existColumnNames.contains(expression->getRawName())) {
            throw BinderException("Multiple result column with the same name " +
                                  expression->getRawName() + " are not supported.");
        }
        existColumnNames.insert(expression->getRawName());
    }
}

void Binder::validateProjectionColumnsInWithClauseAreAliased(const expression_vector& expressions) {
    for (auto& expression : expressions) {
        if (!expression->hasAlias()) {
            throw BinderException("Expression in WITH must be aliased (use AS).");
        }
    }
}

void Binder::validateOrderByFollowedBySkipOrLimitInWithClause(
    const BoundProjectionBody& boundProjectionBody) {
    auto hasSkipOrLimit = boundProjectionBody.hasSkip() || boundProjectionBody.hasLimit();
    if (boundProjectionBody.hasOrderByExpressions() && !hasSkipOrLimit) {
        throw BinderException("In WITH clause, ORDER BY must be followed by SKIP or LIMIT.");
    }
}

void Binder::validateQueryGraphIsConnected(const QueryGraph& queryGraph,
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

void Binder::validateUnionColumnsOfTheSameType(
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

void Binder::validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery) {
    auto unionAllExpressionCounter = 0u;
    for (auto i = 0u; i < regularQuery.getNumSingleQueries() - 1; i++) {
        unionAllExpressionCounter += regularQuery.getIsUnionAll(i);
    }
    if ((0 < unionAllExpressionCounter) &&
        (unionAllExpressionCounter < regularQuery.getNumSingleQueries() - 1)) {
        throw BinderException("Union and union all can't be used together.");
    }
}

void Binder::validateReadNotFollowUpdate(const NormalizedSingleQuery& normalizedSingleQuery) {
    bool hasSeenUpdateClause = false;
    for (auto i = 0u; i < normalizedSingleQuery.getNumQueryParts(); ++i) {
        auto normalizedQueryPart = normalizedSingleQuery.getQueryPart(i);
        if (hasSeenUpdateClause && normalizedQueryPart->getNumQueryGraph() != 0) {
            throw BinderException("Read after update is not supported.");
        }
        hasSeenUpdateClause |= normalizedQueryPart->hasUpdatingClause();
    }
}

void Binder::validateNodeCreateHasPrimaryKeyInput(
    const NodeUpdateInfo& nodeUpdateInfo, const Property& primaryKeyProperty) {
    for (auto i = 0u; i < nodeUpdateInfo.getNumPropertyUpdateInfo(); ++i) {
        auto& property =
            (PropertyExpression&)*nodeUpdateInfo.getPropertyUpdateInfo(i)->getProperty();
        if (property.getPropertyKey() == primaryKeyProperty.propertyID) {
            return;
        }
    }
    throw BinderException("Create node " + nodeUpdateInfo.getNodeExpression()->getRawName() +
                          " expects primary key input.");
}

void Binder::validatePrimaryKey(uint32_t primaryKeyIdx, vector<pair<string, string>> properties) {
    auto primaryKey = properties[primaryKeyIdx];
    // We only support INT64 and STRING column as the primary key.
    if ((primaryKey.second != string("INT64")) && (primaryKey.second != string("STRING"))) {
        throw BinderException("Invalid primary key type: " + primaryKey.second + ".");
    }
}

void Binder::validateTableExist(string& tableName) const {
    if (!catalog.getReadOnlyVersion()->containNodeTable(tableName) &&
        !catalog.getReadOnlyVersion()->containRelTable(tableName)) {
        throw BinderException("Node/Rel " + tableName + " does not exist.");
    }
}

bool Binder::validateStringParsingOptionName(string& parsingOptionName) {
    for (auto i = 0; i < size(CopyCSVConfig::STRING_CSV_PARSING_OPTIONS); i++) {
        if (parsingOptionName == CopyCSVConfig::STRING_CSV_PARSING_OPTIONS[i]) {
            return true;
        }
    }
    return false;
}

string Binder::getUniqueExpressionName(const string& name) {
    return "_" + to_string(lastExpressionId++) + "_" + name;
}

unordered_map<string, shared_ptr<Expression>> Binder::enterSubquery() {
    return variablesInScope;
}

void Binder::exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope) {
    variablesInScope = move(prevVariablesInScope);
}

void Binder::validateNodeTableHasNoEdge(table_id_t tableID) const {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        if (tableIDSchema.second->edgeContainsNodeTable(tableID)) {
            throw BinderException(StringUtils::string_format(
                "Cannot delete a node table with edges. It is on the edges of rel: %s.",
                tableIDSchema.second->tableName.c_str()));
        }
    }
}

} // namespace binder
} // namespace graphflow
