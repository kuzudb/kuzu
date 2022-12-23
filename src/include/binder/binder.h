#pragma once

#include "binder/query/bound_regular_query.h"
#include "common/csv_reader/csv_reader.h"
#include "expression_binder.h"
#include "parser/query/regular_query.h"
#include "query_normalizer.h"

using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

class BoundCreateNode;
class BoundCreateRel;
class BoundSetNodeProperty;
class BoundSetRelProperty;
class BoundDeleteNode;
class SetItem;

class Binder {
    friend class ExpressionBinder;

public:
    explicit Binder(const Catalog& catalog)
        : catalog{catalog}, lastExpressionId{0}, variablesInScope{}, expressionBinder{this} {}

    unique_ptr<BoundStatement> bind(const Statement& statement);

    inline unordered_map<string, shared_ptr<Literal>> getParameterMap() {
        return expressionBinder.parameterMap;
    }

private:
    shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    table_id_t bindRelTableID(const string& tableName) const;
    table_id_t bindNodeTableID(const string& tableName) const;

    shared_ptr<Expression> createVariable(const string& name, const DataType& dataType);

    /*** bind DDL ***/
    unique_ptr<BoundStatement> bindCreateNodeClause(const Statement& statement);
    unique_ptr<BoundStatement> bindCreateRelClause(const Statement& statement);
    unique_ptr<BoundStatement> bindDropTable(const Statement& statement);

    vector<PropertyNameDataType> bindPropertyNameDataTypes(
        vector<pair<string, string>> propertyNameDataTypes);
    uint32_t bindPrimaryKey(string pkColName, vector<pair<string, string>> propertyNameDataTypes);

    /*** bind copy csv ***/
    unique_ptr<BoundStatement> bindCopyCSV(const Statement& statement);

    CSVReaderConfig bindParsingOptions(
        const unordered_map<string, unique_ptr<ParsedExpression>>* parsingOptions);
    void bindStringParsingOptions(
        CSVReaderConfig& csvReaderConfig, const string& optionName, string& optionValue);
    char bindParsingOptionValue(string value);

    /*** bind query ***/
    unique_ptr<BoundRegularQuery> bindQuery(const RegularQuery& regularQuery);
    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);
    unique_ptr<BoundQueryPart> bindQueryPart(const QueryPart& queryPart);

    /*** bind reading clause ***/
    unique_ptr<BoundReadingClause> bindReadingClause(const ReadingClause& readingClause);
    unique_ptr<BoundReadingClause> bindMatchClause(const ReadingClause& readingClause);
    unique_ptr<BoundReadingClause> bindUnwindClause(const ReadingClause& readingClause);

    /*** bind updating clause ***/
    unique_ptr<BoundUpdatingClause> bindUpdatingClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindCreateClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindSetClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindDeleteClause(const UpdatingClause& updatingClause);

    unique_ptr<BoundCreateNode> bindCreateNode(
        shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection);
    unique_ptr<BoundCreateRel> bindCreateRel(
        shared_ptr<RelExpression> rel, const PropertyKeyValCollection& collection);
    unique_ptr<BoundSetNodeProperty> bindSetNodeProperty(
        shared_ptr<NodeExpression> node, pair<ParsedExpression*, ParsedExpression*> setItem);
    unique_ptr<BoundSetRelProperty> bindSetRelProperty(
        shared_ptr<RelExpression> rel, pair<ParsedExpression*, ParsedExpression*> setItem);
    expression_pair bindSetItem(pair<ParsedExpression*, ParsedExpression*> setItem);
    unique_ptr<BoundDeleteNode> bindDeleteNode(shared_ptr<NodeExpression> node);
    shared_ptr<RelExpression> bindDeleteRel(shared_ptr<RelExpression> rel);

    /*** bind projection clause ***/
    unique_ptr<BoundWithClause> bindWithClause(const WithClause& withClause);
    unique_ptr<BoundReturnClause> bindReturnClause(
        const ReturnClause& returnClause, unique_ptr<BoundSingleQuery>& boundSingleQuery);

    expression_vector bindProjectionExpressions(
        const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool containsStar);
    // For RETURN clause, we write variable "v" as all properties of "v"
    expression_vector rewriteProjectionExpressions(const expression_vector& expressions);
    expression_vector rewriteAsAllProperties(
        const shared_ptr<Expression>& expression, DataTypeID nodeOrRelType);

    void bindOrderBySkipLimitIfNecessary(
        BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody);
    expression_vector bindOrderByExpressions(
        const vector<unique_ptr<ParsedExpression>>& orderByExpressions);
    uint64_t bindSkipLimitExpression(const ParsedExpression& expression);

    void addExpressionsToScope(const expression_vector& projectionExpressions);
    void resolveAnyDataTypeWithDefaultType(const expression_vector& expressions);

    /*** bind graph pattern ***/
    pair<unique_ptr<QueryGraphCollection>, unique_ptr<PropertyKeyValCollection>> bindGraphPattern(
        const vector<unique_ptr<PatternElement>>& graphPattern);

    unique_ptr<QueryGraph> bindPatternElement(
        const PatternElement& patternElement, PropertyKeyValCollection& collection);

    void bindQueryRel(const RelPattern& relPattern, const shared_ptr<NodeExpression>& leftNode,
        const shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
        PropertyKeyValCollection& collection);
    shared_ptr<NodeExpression> bindQueryNode(const NodePattern& nodePattern, QueryGraph& queryGraph,
        PropertyKeyValCollection& collection);
    shared_ptr<NodeExpression> createQueryNode(const NodePattern& nodePattern);
    inline vector<table_id_t> bindNodeTableIDs(const vector<string>& tableNames) {
        return bindTableIDs(tableNames, NODE);
    }
    inline vector<table_id_t> bindRelTableIDs(const vector<string>& tableNames) {
        return bindTableIDs(tableNames, REL);
    }
    vector<table_id_t> bindTableIDs(const vector<string>& tableNames, DataTypeID nodeOrRelType);

    /*** validations ***/
    // E.g. Optional MATCH (a) RETURN a.age
    // Although this is doable in Neo4j, I don't think the semantic make a lot of sense because
    // there is nothing to left join on.
    static void validateFirstMatchIsNotOptional(const SingleQuery& singleQuery);

    // E.g. ... RETURN a, b AS a
    static void validateProjectionColumnNamesAreUnique(const expression_vector& expressions);

    // E.g. ... RETURN ID(a)
    static void validateProjectionColumnHasNoInternalType(const expression_vector& expressions);

    // E.g. ... WITH COUNT(*) MATCH ...
    static void validateProjectionColumnsInWithClauseAreAliased(
        const expression_vector& expressions);

    static void validateOrderByFollowedBySkipOrLimitInWithClause(
        const BoundProjectionBody& boundProjectionBody);

    static void validateUnionColumnsOfTheSameType(
        const vector<unique_ptr<BoundSingleQuery>>& boundSingleQueries);

    static void validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery);

    // We don't support read (reading and projection clause) after write since this requires reading
    // updated value and multiple property scan is needed which complicates our planning.
    // e.g. MATCH (a:person) WHERE a.fName='A' SET a.fName='B' RETURN a.fName
    // In the example above, we need to read fName both before and after SET.
    static void validateReturnNotFollowUpdate(const NormalizedSingleQuery& singleQuery);
    static void validateReadNotFollowUpdate(const NormalizedSingleQuery& singleQuery);

    static void validateTableExist(const Catalog& _catalog, string& tableName);

    static bool validateStringParsingOptionName(string& parsingOptionName);

    static void validateNodeTableHasNoEdge(const Catalog& _catalog, table_id_t tableID);

    /*** helpers ***/
    string getUniqueExpressionName(const string& name);

    unordered_map<string, shared_ptr<Expression>> enterSubquery();
    void exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);

private:
    const Catalog& catalog;
    uint32_t lastExpressionId;
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    ExpressionBinder expressionBinder;
};

} // namespace binder
} // namespace kuzu
