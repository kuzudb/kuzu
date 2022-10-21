#pragma once

#include "expression_binder.h"
#include "query_normalizer.h"

#include "src/binder/bound_copy_csv/include/bound_copy_csv.h"
#include "src/binder/bound_ddl/include/bound_create_node_clause.h"
#include "src/binder/bound_ddl/include/bound_create_rel_clause.h"
#include "src/binder/bound_ddl/include/bound_drop_table.h"
#include "src/binder/query/include/bound_regular_query.h"
#include "src/binder/query/reading_clause/include/bound_unwind_clause.h"
#include "src/binder/query/updating_clause/include/bound_create_clause.h"
#include "src/binder/query/updating_clause/include/bound_delete_clause.h"
#include "src/binder/query/updating_clause/include/bound_set_clause.h"
#include "src/parser/copy_csv/include/copy_csv.h"
#include "src/parser/ddl/include/create_node_clause.h"
#include "src/parser/ddl/include/create_rel_clause.h"
#include "src/parser/ddl/include/drop_table.h"
#include "src/parser/query/include/regular_query.h"
#include "src/parser/query/reading_clause/include/unwind_clause.h"

using namespace graphflow::parser;
using namespace graphflow::catalog;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace binder {

class Binder {
    friend class ExpressionBinder;

public:
    explicit Binder(const Catalog& catalog)
        : catalog{catalog}, lastExpressionId{0}, variablesInScope{},
          expressionBinder{this}, logger{LoggerUtils::getOrCreateSpdLogger("binder")} {}

    unique_ptr<BoundStatement> bind(const Statement& statement);

    unique_ptr<BoundRegularQuery> bindQuery(const RegularQuery& regularQuery);

    unique_ptr<BoundCreateNodeClause> bindCreateNodeClause(
        const CreateNodeClause& createNodeClause);

    unique_ptr<BoundCreateRelClause> bindCreateRelClause(const CreateRelClause& createRelClause);

    unique_ptr<BoundCopyCSV> bindCopyCSV(const CopyCSV& copyCSV);

    unique_ptr<BoundDropTable> bindDropTable(const DropTable& dropTable);

    inline unordered_map<string, shared_ptr<Literal>> getParameterMap() {
        return expressionBinder.parameterMap;
    }

private:
    unique_ptr<BoundSingleQuery> bindSingleQuery(const SingleQuery& singleQuery);

    unique_ptr<BoundQueryPart> bindQueryPart(const QueryPart& queryPart);

    unique_ptr<BoundReadingClause> bindReadingClause(const ReadingClause& readingClause);
    unique_ptr<BoundMatchClause> bindMatchClause(const MatchClause& matchClause);
    unique_ptr<BoundUnwindClause> bindUnwindClause(const UnwindClause& unwindClause);

    unique_ptr<BoundUpdatingClause> bindUpdatingClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindCreateClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindSetClause(const UpdatingClause& updatingClause);
    unique_ptr<BoundUpdatingClause> bindDeleteClause(const UpdatingClause& updatingClause);

    unique_ptr<BoundWithClause> bindWithClause(const WithClause& withClause);

    unique_ptr<BoundReturnClause> bindReturnClause(
        const ReturnClause& returnClause, unique_ptr<BoundSingleQuery>& boundSingleQuery);

    expression_vector bindProjectionExpressions(
        const vector<unique_ptr<ParsedExpression>>& projectionExpressions, bool containsStar);

    // For RETURN clause, we write variable "v" as all properties of "v"
    expression_vector rewriteProjectionExpressions(const expression_vector& expressions);

    expression_vector rewriteNodeAsAllProperties(const shared_ptr<Expression>& expression);

    expression_vector rewriteRelAsAllProperties(const shared_ptr<Expression>& expression);

    void bindOrderBySkipLimitIfNecessary(
        BoundProjectionBody& boundProjectionBody, const ProjectionBody& projectionBody);

    expression_vector bindOrderByExpressions(
        const vector<unique_ptr<ParsedExpression>>& orderByExpressions);

    uint64_t bindSkipLimitExpression(const ParsedExpression& expression);

    void addExpressionsToScope(const expression_vector& projectionExpressions);

    shared_ptr<Expression> bindWhereExpression(const ParsedExpression& parsedExpression);

    unique_ptr<QueryGraph> bindQueryGraph(const vector<unique_ptr<PatternElement>>& graphPattern);

    void bindQueryRel(const RelPattern& relPattern, const shared_ptr<NodeExpression>& leftNode,
        const shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph);

    shared_ptr<NodeExpression> bindQueryNode(
        const NodePattern& nodePattern, QueryGraph& queryGraph);
    shared_ptr<NodeExpression> createQueryNode(const NodePattern& nodePattern);

    table_id_t bindRelTable(const string& tableName) const;

    table_id_t bindNodeTableName(const string& tableName) const;

    static uint32_t bindPrimaryKey(
        string pkColName, vector<pair<string, string>> propertyNameDataTypes);

    static vector<PropertyNameDataType> bindPropertyNameDataTypes(
        vector<pair<string, string>> propertyNameDataTypes);

    SrcDstTableIDs bindRelConnections(RelConnection relConnections) const;

    CSVReaderConfig bindParsingOptions(
        const unordered_map<string, unique_ptr<ParsedExpression>>* parsingOptions);

    void bindStringParsingOptions(
        CSVReaderConfig& csvReaderConfig, const string& copyOptionName, string& copyOptionValue);

    static char bindParsingOptionValue(string parsingOptionValue);

    void validateCSVHeader(bool isNodeTable, const vector<string>& csvFields, table_id_t tableID);

    /******* validations *********/
    // E.g. Optional MATCH (a) RETURN a.age
    // Although this is doable in Neo4j, I don't think the semantic make a lot of sense because
    // there is nothing to left join on.
    static void validateFirstMatchIsNotOptional(const SingleQuery& singleQuery);

    // E.g. MATCH (:person)-[:studyAt]->(:person) ...
    static void validateNodeAndRelTableIsConnected(const Catalog& catalog_, table_id_t relTableID,
        table_id_t nodeTableID, RelDirection direction);

    // E.g. ... RETURN a, b AS a
    static void validateProjectionColumnNamesAreUnique(const expression_vector& expressions);

    // E.g. ... WITH COUNT(*) MATCH ...
    static void validateProjectionColumnsInWithClauseAreAliased(
        const expression_vector& expressions);

    static void validateOrderByFollowedBySkipOrLimitInWithClause(
        const BoundProjectionBody& boundProjectionBody);

    static void validateQueryGraphIsConnected(const QueryGraph& queryGraph,
        const unordered_map<string, shared_ptr<Expression>>& prevVariablesInScope);

    static void validateUnionColumnsOfTheSameType(
        const vector<unique_ptr<BoundSingleQuery>>& boundSingleQueries);

    static void validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery);

    static void validateReadNotFollowUpdate(const NormalizedSingleQuery& normalizedSingleQuery);

    static void validateNodeCreateHasPrimaryKeyInput(
        const NodeUpdateInfo& nodeUpdateInfo, const Property& primaryKeyProperty);

    static void validatePrimaryKey(
        string pkColName, uint32_t primaryKeyIdx, vector<pair<string, string>> properties);

    void validateTableExist(string& tableName) const;

    static bool validateStringParsingOptionName(string& parsingOptionName);

    /******* helpers *********/

    string getUniqueExpressionName(const string& name);

    unordered_map<string, shared_ptr<Expression>> enterSubquery();
    void exitSubquery(unordered_map<string, shared_ptr<Expression>> prevVariablesInScope);

    void validateNodeTableHasNoEdge(table_id_t tableID) const;

    static vector<string> getFieldNamesForCSV(
        const string& filePath, const CSVReaderConfig& csvReaderConfig);
    static bool compareStringsCaseInsensitive(const string& str1, const string& str2);

private:
    const Catalog& catalog;
    uint32_t lastExpressionId;
    unordered_map<string, shared_ptr<Expression>> variablesInScope;
    ExpressionBinder expressionBinder;
    shared_ptr<spdlog::logger> logger;
};

} // namespace binder
} // namespace graphflow
