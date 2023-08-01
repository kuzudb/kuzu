#pragma once

#include "binder/query/bound_regular_query.h"
#include "common/copier_config/copier_config.h"
#include "expression_binder.h"
#include "parser/query/regular_query.h"
#include "query_normalizer.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace binder {

class BoundCreateInfo;
class BoundSetPropertyInfo;
class BoundDeleteInfo;

// BinderScope keeps track of expressions in scope and their aliases. We maintain the order of
// expressions in
class BinderScope {
public:
    BinderScope() = default;
    BinderScope(expression_vector expressions,
        std::unordered_map<std::string, common::vector_idx_t> varNameToIdx)
        : expressions{std::move(expressions)}, varNameToIdx{std::move(varNameToIdx)} {}

    inline bool empty() const { return expressions.empty(); }
    inline bool contains(const std::string& varName) const {
        return varNameToIdx.contains(varName);
    }
    inline std::shared_ptr<Expression> getExpression(const std::string& varName) const {
        return expressions[varNameToIdx.at(varName)];
    }
    inline expression_vector getExpressions() const { return expressions; }
    inline void addExpression(const std::string& varName, std::shared_ptr<Expression> expression) {
        varNameToIdx.insert({varName, expressions.size()});
        expressions.push_back(std::move(expression));
    }
    inline void clear() {
        expressions.clear();
        varNameToIdx.clear();
    }
    inline std::unique_ptr<BinderScope> copy() {
        return std::make_unique<BinderScope>(expressions, varNameToIdx);
    }

private:
    expression_vector expressions;
    std::unordered_map<std::string, common::vector_idx_t> varNameToIdx;
};

class Binder {
    friend class ExpressionBinder;

public:
    explicit Binder(const catalog::Catalog& catalog, main::ClientContext* clientContext)
        : catalog{catalog}, lastExpressionId{0}, scope{std::make_unique<BinderScope>()},
          expressionBinder{this}, clientContext{clientContext} {}

    std::unique_ptr<BoundStatement> bind(const parser::Statement& statement);

    inline std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return expressionBinder.parameterMap;
    }

private:
    std::shared_ptr<Expression> bindWhereExpression(
        const parser::ParsedExpression& parsedExpression);

    common::table_id_t bindRelTableID(const std::string& tableName) const;
    common::table_id_t bindNodeTableID(const std::string& tableName) const;

    std::shared_ptr<Expression> createVariable(
        const std::string& name, const common::LogicalType& dataType);

    /*** bind DDL ***/
    std::unique_ptr<BoundStatement> bindCreateNodeTableClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCreateRelTableClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDropTableClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameTableClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindAddPropertyClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDropPropertyClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenamePropertyClause(const parser::Statement& statement);

    std::vector<catalog::Property> bindProperties(
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes);
    uint32_t bindPrimaryKey(const std::string& pkColName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes);
    common::property_id_t bindPropertyName(
        catalog::NodeTableSchema::TableSchema* tableSchema, const std::string& propertyName);
    common::LogicalType bindDataType(const std::string& dataType);

    /*** bind copy csv ***/
    std::unique_ptr<BoundStatement> bindCopyClause(const parser::Statement& statement);

    std::vector<std::string> bindFilePaths(const std::vector<std::string>& filePaths);

    common::CSVReaderConfig bindParsingOptions(
        const std::unordered_map<std::string, std::unique_ptr<parser::ParsedExpression>>*
            parsingOptions);
    void bindStringParsingOptions(common::CSVReaderConfig& csvReaderConfig,
        const std::string& optionName, std::string& optionValue);
    char bindParsingOptionValue(std::string value);
    common::CopyDescription::FileType bindFileType(std::vector<std::string> filePaths);

    /*** bind query ***/
    std::unique_ptr<BoundRegularQuery> bindQuery(const parser::RegularQuery& regularQuery);
    std::unique_ptr<BoundSingleQuery> bindSingleQuery(const parser::SingleQuery& singleQuery);
    std::unique_ptr<BoundQueryPart> bindQueryPart(const parser::QueryPart& queryPart);

    /*** bind call ***/
    std::unique_ptr<BoundStatement> bindStandaloneCall(const parser::Statement& statement);

    /*** bind create macro ***/
    std::unique_ptr<BoundStatement> bindCreateMacro(const parser::Statement& statement);

    /*** bind explain ***/
    std::unique_ptr<BoundStatement> bindExplain(const parser::Statement& statement);

    /*** bind reading clause ***/
    std::unique_ptr<BoundReadingClause> bindReadingClause(
        const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindMatchClause(const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindUnwindClause(
        const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindInQueryCall(const parser::ReadingClause& readingClause);

    /*** bind updating clause ***/
    std::unique_ptr<BoundUpdatingClause> bindUpdatingClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindCreateClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindMergeClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindSetClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindDeleteClause(
        const parser::UpdatingClause& updatingClause);

    std::vector<std::unique_ptr<BoundCreateInfo>> bindCreateInfos(
        const QueryGraphCollection& queryGraphCollection,
        const PropertyKeyValCollection& keyValCollection, const expression_set& nodeRelScope_);
    std::unique_ptr<BoundCreateInfo> bindCreateNodeInfo(
        std::shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection);
    std::unique_ptr<BoundCreateInfo> bindCreateRelInfo(
        std::shared_ptr<RelExpression> rel, const PropertyKeyValCollection& collection);
    std::unique_ptr<BoundSetPropertyInfo> bindSetPropertyInfo(
        std::pair<parser::ParsedExpression*, parser::ParsedExpression*> setItem);
    expression_pair bindSetItem(
        std::pair<parser::ParsedExpression*, parser::ParsedExpression*> setItem);
    std::unique_ptr<BoundDeleteInfo> bindDeleteNodeInfo(std::shared_ptr<NodeExpression> node);
    std::unique_ptr<BoundDeleteInfo> bindDeleteRelInfo(std::shared_ptr<RelExpression> rel);

    /*** bind projection clause ***/
    std::unique_ptr<BoundWithClause> bindWithClause(const parser::WithClause& withClause);
    std::unique_ptr<BoundReturnClause> bindReturnClause(const parser::ReturnClause& returnClause);
    std::unique_ptr<BoundProjectionBody> bindProjectionBody(
        const parser::ProjectionBody& projectionBody,
        const expression_vector& projectionExpressions);

    expression_vector bindProjectionExpressions(
        const parser::parsed_expression_vector& parsedExpressions);

    expression_vector bindOrderByExpressions(
        const std::vector<std::unique_ptr<parser::ParsedExpression>>& orderByExpressions);
    uint64_t bindSkipLimitExpression(const parser::ParsedExpression& expression);

    void addExpressionsToScope(const expression_vector& projectionExpressions);
    void resolveAnyDataTypeWithDefaultType(const expression_vector& expressions);

    /*** bind graph pattern ***/
    std::pair<std::unique_ptr<QueryGraphCollection>, std::unique_ptr<PropertyKeyValCollection>>
    bindGraphPattern(const std::vector<std::unique_ptr<parser::PatternElement>>& graphPattern);

    std::unique_ptr<QueryGraph> bindPatternElement(
        const parser::PatternElement& patternElement, PropertyKeyValCollection& collection);
    std::shared_ptr<Expression> createPathExpression(
        const std::string& pathName, const expression_vector& children);

    std::shared_ptr<RelExpression> bindQueryRel(const parser::RelPattern& relPattern,
        const std::shared_ptr<NodeExpression>& leftNode,
        const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
        PropertyKeyValCollection& collection);
    std::shared_ptr<RelExpression> createNonRecursiveQueryRel(const std::string& parsedName,
        const std::vector<common::table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType);
    std::shared_ptr<RelExpression> createRecursiveQueryRel(const parser::RelPattern& relPattern,
        const std::vector<common::table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType);
    std::pair<uint64_t, uint64_t> bindVariableLengthRelBound(const parser::RelPattern& relPattern);
    void bindQueryRelProperties(RelExpression& rel);

    std::shared_ptr<NodeExpression> bindQueryNode(const parser::NodePattern& nodePattern,
        QueryGraph& queryGraph, PropertyKeyValCollection& collection);
    std::shared_ptr<NodeExpression> createQueryNode(const parser::NodePattern& nodePattern);
    std::shared_ptr<NodeExpression> createQueryNode(
        const std::string& parsedName, const std::vector<common::table_id_t>& tableIDs);
    void bindQueryNodeProperties(NodeExpression& node);

    std::vector<common::table_id_t> bindNodeTableIDs(const std::vector<std::string>& tableNames);
    std::vector<common::table_id_t> bindRelTableIDs(const std::vector<std::string>& tableNames);

    /*** validations ***/
    // E.g. ... RETURN a, b AS a
    static void validateProjectionColumnNamesAreUnique(const expression_vector& expressions);

    // E.g. ... WITH COUNT(*) MATCH ...
    static void validateProjectionColumnsInWithClauseAreAliased(
        const expression_vector& expressions);

    static void validateOrderByFollowedBySkipOrLimitInWithClause(
        const BoundProjectionBody& boundProjectionBody);

    static void validateUnionColumnsOfTheSameType(
        const std::vector<std::unique_ptr<BoundSingleQuery>>& boundSingleQueries);

    static void validateIsAllUnionOrUnionAll(const BoundRegularQuery& regularQuery);

    // We don't support read (reading and projection clause) after write since this requires reading
    // updated value and multiple property scan is needed which complicates our planning.
    // e.g. MATCH (a:person) WHERE a.fName='A' SET a.fName='B' RETURN a.fName
    // In the example above, we need to read fName both before and after SET.
    static void validateReturnNotFollowUpdate(const NormalizedSingleQuery& singleQuery);
    static void validateReadNotFollowUpdate(const NormalizedSingleQuery& singleQuery);

    static void validateTableExist(const catalog::Catalog& _catalog, std::string& tableName);

    static bool validateStringParsingOptionName(std::string& parsingOptionName);

    static void validateNodeTableHasNoEdge(
        const catalog::Catalog& _catalog, common::table_id_t tableID);

    /*** helpers ***/
    std::string getUniqueExpressionName(const std::string& name);

    std::unique_ptr<BinderScope> saveScope();
    void restoreScope(std::unique_ptr<BinderScope> prevVariableScope);

private:
    const catalog::Catalog& catalog;
    uint32_t lastExpressionId;
    std::unique_ptr<BinderScope> scope;
    ExpressionBinder expressionBinder;
    main::ClientContext* clientContext;
};

} // namespace binder
} // namespace kuzu
