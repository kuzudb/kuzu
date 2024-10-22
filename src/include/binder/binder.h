#pragma once

#include "binder/binder_scope.h"
#include "binder/expression_binder.h"
#include "binder/query/bound_regular_query.h"
#include "binder/query/query_graph.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/copier_config/reader_config.h"
#include "common/enums/table_type.h"
#include "graph/graph_entry.h"
#include "parser/ddl/parsed_property_definition.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "parser/query/regular_query.h"

namespace kuzu {
namespace parser {
struct PropertyDefinitionDDL;
struct CreateTableInfo;
struct BaseScanSource;
class ProjectGraph;
struct JoinHintNode;
} // namespace parser

namespace catalog {
class NodeTableCatalogEntry;
class RelTableCatalogEntry;
class RDFGraphCatalogEntry;
class Catalog;
} // namespace catalog

namespace extension {
struct ExtensionOptions;
} // namespace extension

namespace graph {
struct GraphEntry;
}

namespace main {
class ClientContext;
class Database;
} // namespace main

namespace function {
struct TableFunction;
} // namespace function

namespace transaction {
class Transaction;
} // namespace transaction

namespace common {
struct RdfReaderConfig;
}

namespace binder {
struct BoundBaseScanSource;
struct BoundCreateTableInfo;
struct BoundInsertInfo;
struct BoundSetPropertyInfo;
struct BoundDeleteInfo;
class BoundWithClause;
class BoundReturnClause;
struct ExportedTableData;
struct BoundJoinHintNode;
struct BoundCopyFromInfo;

// BinderScope keeps track of expressions in scope and their aliases. We maintain the order of
// expressions in

class Binder {
    friend class ExpressionBinder;

public:
    explicit Binder(main::ClientContext* clientContext)
        : lastExpressionId{0}, scope{}, graphEntrySet{}, expressionBinder{this, clientContext},
          clientContext{clientContext} {}

    std::unique_ptr<BoundStatement> bind(const parser::Statement& statement);

    void setInputParameters(
        std::unordered_map<std::string, std::shared_ptr<common::Value>> parameters) {
        expressionBinder.parameterMap = parameters;
    }

    std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return expressionBinder.parameterMap;
    }

    bool bindExportTableData(ExportedTableData& tableData, const catalog::TableCatalogEntry& entry,
        const catalog::Catalog& catalog, transaction::Transaction* tx);
    std::shared_ptr<Expression> createVariable(const std::string& name,
        const common::LogicalType& dataType);

    std::shared_ptr<Expression> bindWhereExpression(
        const parser::ParsedExpression& parsedExpression);

    std::shared_ptr<Expression> createVariable(std::string_view name, common::LogicalTypeID typeID);
    std::shared_ptr<Expression> createVariable(const std::string& name,
        common::LogicalTypeID typeID);

    /*** bind DDL ***/
    BoundCreateTableInfo bindCreateTableInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateNodeTableInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateRelTableInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateRelTableGroupInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateRdfGraphInfo(const parser::CreateTableInfo* info);
    std::unique_ptr<BoundStatement> bindCreateTable(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCreateType(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCreateSequence(const parser::Statement& statement);

    std::unique_ptr<BoundStatement> bindDrop(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindAlter(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameTable(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindAddProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDropProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCommentOn(const parser::Statement& statement);

    std::vector<PropertyDefinition> bindPropertyDefinitions(
        const std::vector<parser::ParsedPropertyDefinition>& parsedDefinitions,
        const std::string& tableName);

    /*** bind copy ***/
    std::unique_ptr<BoundStatement> bindCopyFromClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCopyNodeFrom(const parser::Statement& statement,
        catalog::NodeTableCatalogEntry* nodeTableEntry);
    std::unique_ptr<BoundStatement> bindCopyRelFrom(const parser::Statement& statement,
        catalog::RelTableCatalogEntry* relTableEntry);
    std::unique_ptr<BoundStatement> bindCopyRdfFrom(const parser::Statement& statement,
        catalog::RDFGraphCatalogEntry* rdfGraphEntry);
    BoundCopyFromInfo bindCopyRdfResourceInfo(const common::RdfReaderConfig& config,
        const function::TableFuncBindData& bindData, const catalog::RDFGraphCatalogEntry& rdfEntry);
    BoundCopyFromInfo bindCopyRdfLiteralInfo(const common::RdfReaderConfig& config,
        const function::TableFuncBindData& bindData, const catalog::RDFGraphCatalogEntry& rdfEntry);
    BoundCopyFromInfo bindCopyRdfResourceTriplesInfo(const common::RdfReaderConfig& config,
        const function::TableFuncBindData& bindData, const catalog::RDFGraphCatalogEntry& rdfEntry);
    BoundCopyFromInfo bindCopyRdfLiteralTriplesInfo(const common::RdfReaderConfig& config,
        const function::TableFuncBindData& bindData, const catalog::RDFGraphCatalogEntry& rdfEntry);

    std::unique_ptr<BoundStatement> bindCopyToClause(const parser::Statement& statement);

    std::unique_ptr<BoundStatement> bindExportDatabaseClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindImportDatabaseClause(const parser::Statement& statement);

    std::unique_ptr<BoundStatement> bindAttachDatabase(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDetachDatabase(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindUseDatabase(const parser::Statement& statement);

    /*** bind scan source ***/
    std::unique_ptr<BoundBaseScanSource> bindScanSource(parser::BaseScanSource* scanSource,
        const parser::options_t& options, const std::vector<std::string>& columnNames,
        const std::vector<common::LogicalType>& columnTypes);
    std::unique_ptr<BoundBaseScanSource> bindFileScanSource(
        const parser::BaseScanSource& scanSource, const parser::options_t& options,
        const std::vector<std::string>& columnNames,
        const std::vector<common::LogicalType>& columnTypes);
    std::unique_ptr<BoundBaseScanSource> bindQueryScanSource(
        const parser::BaseScanSource& scanSource, const std::vector<std::string>& columnNames,
        const std::vector<common::LogicalType>& columnTypes);
    std::unique_ptr<BoundBaseScanSource> bindObjectScanSource(
        const parser::BaseScanSource& scanSource, const parser::options_t& options,
        const std::vector<std::string>& columnNames,
        const std::vector<common::LogicalType>& columnTypes);

    std::unordered_map<std::string, common::Value> bindParsingOptions(
        const parser::options_t& parsingOptions);
    common::FileTypeInfo bindFileTypeInfo(const std::vector<std::string>& filePaths);
    std::vector<std::string> bindFilePaths(const std::vector<std::string>& filePaths);

    /*** bind query ***/
    std::unique_ptr<BoundRegularQuery> bindQuery(const parser::RegularQuery& regularQuery);
    NormalizedSingleQuery bindSingleQuery(const parser::SingleQuery& singleQuery);
    NormalizedQueryPart bindQueryPart(const parser::QueryPart& queryPart);

    graph::GraphEntry bindProjectGraph(const parser::ProjectGraph& projectGraph);

    /*** bind standalone call ***/
    std::unique_ptr<BoundStatement> bindStandaloneCall(const parser::Statement& statement);

    /*** bind standalone call function ***/
    std::unique_ptr<BoundStatement> bindStandaloneCallFunction(const parser::Statement& statement);

    /*** bind create macro ***/
    std::unique_ptr<BoundStatement> bindCreateMacro(const parser::Statement& statement);

    /*** bind transaction ***/
    std::unique_ptr<BoundStatement> bindTransaction(const parser::Statement& statement);

    /*** bind extension ***/
    std::unique_ptr<BoundStatement> bindExtension(const parser::Statement& statement);

    /*** bind explain ***/
    std::unique_ptr<BoundStatement> bindExplain(const parser::Statement& statement);

    /*** bind reading clause ***/
    std::unique_ptr<BoundReadingClause> bindReadingClause(
        const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindMatchClause(const parser::ReadingClause& readingClause);
    std::shared_ptr<BoundJoinHintNode> bindJoinHint(const parser::JoinHintNode& joinHintNode);
    void rewriteMatchPattern(BoundGraphPattern& boundGraphPattern);
    std::unique_ptr<BoundReadingClause> bindUnwindClause(
        const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindInQueryCall(const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindLoadFrom(const parser::ReadingClause& readingClause);

    /*** bind updating clause ***/
    std::unique_ptr<BoundUpdatingClause> bindUpdatingClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindInsertClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindMergeClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindSetClause(
        const parser::UpdatingClause& updatingClause);
    std::unique_ptr<BoundUpdatingClause> bindDeleteClause(
        const parser::UpdatingClause& updatingClause);

    std::vector<BoundInsertInfo> bindInsertInfos(QueryGraphCollection& queryGraphCollection,
        const std::unordered_set<std::string>& patternsInScope);
    void bindInsertNode(std::shared_ptr<NodeExpression> node, std::vector<BoundInsertInfo>& infos);
    void bindInsertRel(std::shared_ptr<RelExpression> rel, std::vector<BoundInsertInfo>& infos);
    expression_vector bindInsertColumnDataExprs(
        const common::case_insensitive_map_t<std::shared_ptr<Expression>>& propertyDataExpr,
        const std::vector<PropertyDefinition>& propertyDefinitions);

    BoundSetPropertyInfo bindSetPropertyInfo(parser::ParsedExpression* column,
        parser::ParsedExpression* columnData);
    expression_pair bindSetItem(parser::ParsedExpression* column,
        parser::ParsedExpression* columnData);

    /*** bind projection clause ***/
    BoundWithClause bindWithClause(const parser::WithClause& withClause);
    BoundReturnClause bindReturnClause(const parser::ReturnClause& returnClause);

    std::pair<expression_vector, std::vector<std::string>> bindProjectionList(
        const parser::ProjectionBody& projectionBody);
    BoundProjectionBody bindProjectionBody(const parser::ProjectionBody& projectionBody,
        const expression_vector& projectionExprs, const std::vector<std::string>& aliases);

    expression_vector bindOrderByExpressions(
        const std::vector<std::unique_ptr<parser::ParsedExpression>>& parsedExprs);
    uint64_t bindSkipLimitExpression(const parser::ParsedExpression& expression);

    /*** bind graph pattern ***/
    BoundGraphPattern bindGraphPattern(const std::vector<parser::PatternElement>& graphPattern);

    QueryGraph bindPatternElement(const parser::PatternElement& patternElement);
    std::shared_ptr<Expression> createPath(const std::string& pathName,
        const expression_vector& children);

    std::shared_ptr<RelExpression> bindQueryRel(const parser::RelPattern& relPattern,
        const std::shared_ptr<NodeExpression>& leftNode,
        const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph);
    std::shared_ptr<RelExpression> createNonRecursiveQueryRel(const std::string& parsedName,
        const std::vector<catalog::TableCatalogEntry*>& entries,
        std::shared_ptr<NodeExpression> srcNode, std::shared_ptr<NodeExpression> dstNode,
        RelDirectionType directionType);
    std::shared_ptr<RelExpression> createRecursiveQueryRel(const parser::RelPattern& relPattern,
        const std::vector<catalog::TableCatalogEntry*>& entries,
        std::shared_ptr<NodeExpression> srcNode, std::shared_ptr<NodeExpression> dstNode,
        RelDirectionType directionType);
    expression_vector bindRecursivePatternNodeProjectionList(
        const parser::RecursiveRelPatternInfo& info, const NodeOrRelExpression& expr);
    expression_vector bindRecursivePatternRelProjectionList(
        const parser::RecursiveRelPatternInfo& info, const NodeOrRelExpression& expr);
    std::pair<uint64_t, uint64_t> bindVariableLengthRelBound(const parser::RelPattern& relPattern);
    void bindQueryRelProperties(RelExpression& rel);

    std::shared_ptr<NodeExpression> bindQueryNode(const parser::NodePattern& nodePattern,
        QueryGraph& queryGraph);
    std::shared_ptr<NodeExpression> createQueryNode(const parser::NodePattern& nodePattern);
    std::shared_ptr<NodeExpression> createQueryNode(const std::string& parsedName,
        const std::vector<catalog::TableCatalogEntry*>& entries);
    void bindQueryNodeProperties(NodeExpression& node);

    /*** bind table entries ***/
    std::vector<catalog::TableCatalogEntry*> bindTableEntries(
        const std::vector<std::string>& tableNames, bool nodePattern) const;
    catalog::TableCatalogEntry* bindTableEntry(const std::string& tableName) const;
    common::table_id_t bindTableID(const std::string& tableName) const;
    std::vector<catalog::TableCatalogEntry*> getNodeTableEntries(
        const std::vector<catalog::TableCatalogEntry*>& entries) const;
    std::vector<catalog::TableCatalogEntry*> getRelTableEntries(
        const std::vector<catalog::TableCatalogEntry*>& entries) const;
    std::vector<catalog::TableCatalogEntry*> getTableEntries(
        const std::vector<catalog::TableCatalogEntry*>& entries, common::TableType tableType) const;
    std::vector<catalog::TableCatalogEntry*> getNodeTableEntries(
        catalog::TableCatalogEntry* entry) const;
    std::vector<catalog::TableCatalogEntry*> getRelTableEntries(
        catalog::TableCatalogEntry* entry) const;
    // TODO(Xiyang): remove id based table binding logic.
    std::vector<catalog::TableCatalogEntry*> getTableEntries(
        const common::table_id_vector_t& tableIDs);

    /*** validations ***/
    static void validateOrderByFollowedBySkipOrLimitInWithClause(
        const BoundProjectionBody& boundProjectionBody);

    void validateTableType(common::table_id_t tableID, common::TableType expectedTableType);
    void validateTableExist(const std::string& tableName);
    void validateDropTable(const parser::Statement& dropTable);
    void validateDropSequence(const parser::Statement& dropTable);
    /*** helpers ***/
    std::string getUniqueExpressionName(const std::string& name);

    static bool reservedInColumnName(const std::string& name);
    static bool reservedInPropertyLookup(const std::string& name);

    void addToScope(const std::vector<std::string>& names, const expression_vector& exprs);
    void addToScope(const std::string& name, std::shared_ptr<Expression> expr);
    BinderScope saveScope();
    void restoreScope(BinderScope prevScope);

    function::TableFunction getScanFunction(common::FileTypeInfo typeInfo,
        const common::ReaderConfig& config);

    ExpressionBinder* getExpressionBinder() { return &expressionBinder; }

private:
    uint32_t lastExpressionId;
    BinderScope scope;
    graph::GraphEntrySet graphEntrySet;
    ExpressionBinder expressionBinder;
    main::ClientContext* clientContext;
};

} // namespace binder
} // namespace kuzu
