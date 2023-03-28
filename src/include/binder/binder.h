#pragma once

#include "binder/query/bound_regular_query.h"
#include "common/copier_config/copier_config.h"
#include "expression_binder.h"
#include "parser/query/regular_query.h"
#include "query_normalizer.h"

namespace kuzu {
namespace binder {

class BoundCreateNode;
class BoundCreateRel;
class BoundSetNodeProperty;
class BoundSetRelProperty;
class BoundDeleteNode;

class Binder {
    friend class ExpressionBinder;

public:
    explicit Binder(const catalog::Catalog& catalog)
        : catalog{catalog}, lastExpressionId{0}, variablesInScope{}, expressionBinder{this} {}

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
        const std::string& name, const common::DataType& dataType);

    /*** bind DDL ***/
    std::unique_ptr<BoundStatement> bindCreateNodeClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCreateRelClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDropTable(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameTable(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindAddProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDropProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameProperty(const parser::Statement& statement);

    std::vector<catalog::PropertyNameDataType> bindPropertyNameDataTypes(
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes);
    uint32_t bindPrimaryKey(std::string pkColName,
        std::vector<std::pair<std::string, std::string>> propertyNameDataTypes);
    common::property_id_t bindPropertyName(
        catalog::NodeTableSchema::TableSchema* tableSchema, const std::string& propertyName);
    common::DataType bindDataType(const std::string& dataType);

    /*** bind copy csv ***/
    std::unique_ptr<BoundStatement> bindCopy(const parser::Statement& statement);

    std::vector<std::string> bindFilePaths(const std::vector<std::string>& filePaths);

    std::unordered_map<common::property_id_t, std::string> bindPropertyToNpyMap(
        common::table_id_t tableId, const std::vector<std::string>& filePaths);

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

    /*** bind reading clause ***/
    std::unique_ptr<BoundReadingClause> bindReadingClause(
        const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindMatchClause(const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindUnwindClause(
        const parser::ReadingClause& readingClause);

    /*** bind updating clause ***/
    std::unique_ptr<BoundUpdatingClause> bindUpdatingClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindCreateClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindSetClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindDeleteClause(
        const parser::UpdatingClause& updatingClause);

    std::unique_ptr<BoundCreateNode> bindCreateNode(
        std::shared_ptr<NodeExpression> node, const PropertyKeyValCollection& collection);
    std::unique_ptr<BoundCreateRel> bindCreateRel(
        std::shared_ptr<RelExpression> rel, const PropertyKeyValCollection& collection);
    std::unique_ptr<BoundSetNodeProperty> bindSetNodeProperty(std::shared_ptr<NodeExpression> node,
        std::pair<parser::ParsedExpression*, parser::ParsedExpression*> setItem);
    std::unique_ptr<BoundSetRelProperty> bindSetRelProperty(std::shared_ptr<RelExpression> rel,
        std::pair<parser::ParsedExpression*, parser::ParsedExpression*> setItem);
    expression_pair bindSetItem(
        std::pair<parser::ParsedExpression*, parser::ParsedExpression*> setItem);
    std::unique_ptr<BoundDeleteNode> bindDeleteNode(const std::shared_ptr<NodeExpression>& node);
    std::shared_ptr<RelExpression> bindDeleteRel(std::shared_ptr<RelExpression> rel);

    /*** bind projection clause ***/
    std::unique_ptr<BoundWithClause> bindWithClause(const parser::WithClause& withClause);
    std::unique_ptr<BoundReturnClause> bindReturnClause(const parser::ReturnClause& returnClause);

    expression_vector bindProjectionExpressions(
        const std::vector<std::unique_ptr<parser::ParsedExpression>>& projectionExpressions,
        bool containsStar);
    // Rewrite variable "v" as all properties of "v"
    expression_vector rewriteNodeOrRelExpression(const Expression& expression);
    expression_vector rewriteNodeExpression(const Expression& expression);
    expression_vector rewriteRelExpression(const Expression& expression);

    void bindOrderBySkipLimitIfNecessary(
        BoundProjectionBody& boundProjectionBody, const parser::ProjectionBody& projectionBody);
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

    void bindQueryRel(const parser::RelPattern& relPattern,
        const std::shared_ptr<NodeExpression>& leftNode,
        const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
        PropertyKeyValCollection& collection);
    std::pair<uint64_t, uint64_t> bindVariableLengthRelBound(const parser::RelPattern& relPattern);
    std::shared_ptr<NodeExpression> bindQueryNode(const parser::NodePattern& nodePattern,
        QueryGraph& queryGraph, PropertyKeyValCollection& collection);
    std::shared_ptr<NodeExpression> createQueryNode(const parser::NodePattern& nodePattern);
    inline std::vector<common::table_id_t> bindNodeTableIDs(
        const std::vector<std::string>& tableNames) {
        return bindTableIDs(tableNames, common::NODE);
    }
    inline std::vector<common::table_id_t> bindRelTableIDs(
        const std::vector<std::string>& tableNames) {
        return bindTableIDs(tableNames, common::REL);
    }
    std::vector<common::table_id_t> bindTableIDs(
        const std::vector<std::string>& tableNames, common::DataTypeID nodeOrRelType);

    /*** validations ***/
    // E.g. Optional MATCH (a) RETURN a.age
    // Although this is doable in Neo4j, I don't think the semantic make a lot of sense because
    // there is nothing to left join on.
    static void validateFirstMatchIsNotOptional(const parser::SingleQuery& singleQuery);

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

    std::unordered_map<std::string, std::shared_ptr<Expression>> enterSubquery();
    void exitSubquery(
        std::unordered_map<std::string, std::shared_ptr<Expression>> prevVariablesInScope);

private:
    const catalog::Catalog& catalog;
    uint32_t lastExpressionId;
    std::unordered_map<std::string, std::shared_ptr<Expression>> variablesInScope;
    ExpressionBinder expressionBinder;
};

} // namespace binder
} // namespace kuzu
