#pragma once

#include "binder/query/bound_regular_query.h"
#include "binder/query/query_graph.h"
#include "catalog/catalog.h"
#include "common/copier_config/reader_config.h"
#include "expression_binder.h"
#include "parser/query/graph_pattern/pattern_element.h"
#include "parser/query/regular_query.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace parser {
struct CreateTableInfo;
}

namespace extension {
struct ExtensionOptions;
}

namespace main {
class ClientContext;
class Database;
} // namespace main

namespace function {
struct TableFunction;
}

namespace binder {

struct BoundInsertInfo;
struct BoundSetPropertyInfo;
struct BoundDeleteInfo;
class BoundWithClause;
class BoundReturnClause;
struct BoundFileScanInfo;
struct ExportedTableData;

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
    inline void removeExpression(const std::string& name) {
        auto idx = varNameToIdx.at(name);
        varNameToIdx.erase(name);
        expressions[idx] = nullptr;
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
    explicit Binder(const catalog::Catalog& catalog, storage::MemoryManager* memoryManager,
        storage::StorageManager* storageManager, common::VirtualFileSystem* vfs,
        main::ClientContext* clientContext, extension::ExtensionOptions* extensionOptions)
        : catalog{catalog}, memoryManager{memoryManager}, storageManager{storageManager}, vfs{vfs},
          lastExpressionId{0}, scope{std::make_unique<BinderScope>()}, expressionBinder{this},
          clientContext{clientContext}, extensionOptions{extensionOptions} {}

    std::unique_ptr<BoundStatement> bind(const parser::Statement& statement);

    inline std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return expressionBinder.parameterMap;
    }

    static std::unique_ptr<common::LogicalType> bindDataType(const std::string& dataType);

    ExportedTableData extractExportData(std::string selQuery, std::string tableName);

private:
    std::shared_ptr<Expression> bindWhereExpression(
        const parser::ParsedExpression& parsedExpression);

    common::table_id_t bindTableID(const std::string& tableName) const;

    std::shared_ptr<Expression> createVariable(std::string_view name, common::LogicalTypeID typeID);
    std::shared_ptr<Expression> createVariable(
        const std::string& name, common::LogicalTypeID typeID);
    std::shared_ptr<Expression> createVariable(
        const std::string& name, const common::LogicalType& dataType);

    /*** bind DDL ***/
    BoundCreateTableInfo bindCreateTableInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateNodeTableInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateRelTableInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateRelTableGroupInfo(const parser::CreateTableInfo* info);
    BoundCreateTableInfo bindCreateRdfGraphInfo(const parser::CreateTableInfo* info);
    std::unique_ptr<BoundStatement> bindCreateTable(const parser::Statement& statement);

    std::unique_ptr<BoundStatement> bindDropTable(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindAlter(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameTable(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindAddProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindDropProperty(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindRenameProperty(const parser::Statement& statement);

    std::vector<PropertyInfo> bindPropertyInfo(
        const std::vector<std::pair<std::string, std::string>>& propertyNameDataTypes);

    /*** bind copy ***/
    std::unique_ptr<BoundStatement> bindCopyFromClause(const parser::Statement& statement);
    std::unique_ptr<BoundStatement> bindCopyNodeFrom(const parser::Statement& statement,
        std::unique_ptr<common::ReaderConfig> config,
        catalog::NodeTableCatalogEntry* nodeTableEntry);
    std::unique_ptr<BoundStatement> bindCopyRelFrom(const parser::Statement& statement,
        std::unique_ptr<common::ReaderConfig> config, catalog::RelTableCatalogEntry* relTableEntry);
    std::unique_ptr<BoundStatement> bindCopyRdfFrom(const parser::Statement& statement,
        std::unique_ptr<common::ReaderConfig> config, catalog::RDFGraphCatalogEntry* rdfGraphEntry);
    void bindExpectedNodeColumns(catalog::NodeTableCatalogEntry* nodeTableEntry,
        const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
        std::vector<common::LogicalType>& columnTypes);
    void bindExpectedRelColumns(catalog::RelTableCatalogEntry* relTableEntry,
        const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
        std::vector<common::LogicalType>& columnTypes);

    std::unique_ptr<BoundStatement> bindCopyToClause(const parser::Statement& statement);

    std::unique_ptr<BoundStatement> bindExportDatabaseClause(const parser::Statement& statement);

    /*** bind file scan ***/
    std::unordered_map<std::string, common::Value> bindParsingOptions(
        const parser::parsing_option_t& parsingOptions);
    common::FileType bindFileType(const std::vector<std::string>& filePaths);
    common::FileType bindFileType(const std::string& filePath);
    std::vector<std::string> bindFilePaths(const std::vector<std::string>& filePaths);

    /*** bind query ***/
    std::unique_ptr<BoundRegularQuery> bindQuery(const parser::RegularQuery& regularQuery);
    NormalizedSingleQuery bindSingleQuery(const parser::SingleQuery& singleQuery);
    NormalizedQueryPart bindQueryPart(const parser::QueryPart& queryPart);

    /*** bind call ***/
    std::unique_ptr<BoundStatement> bindStandaloneCall(const parser::Statement& statement);

    /*** bind create macro ***/
    std::unique_ptr<BoundStatement> bindCreateMacro(const parser::Statement& statement);

    /*** bind transaction ***/
    std::unique_ptr<BoundStatement> bindTransaction(const parser::Statement& statement);

    /*** bind extension ***/
    std::unique_ptr<BoundStatement> bindExtension(const parser::Statement& statement);

    /*** bind comment on ***/
    std::unique_ptr<BoundStatement> bindCommentOn(const parser::Statement& statement);

    /*** bind explain ***/
    std::unique_ptr<BoundStatement> bindExplain(const parser::Statement& statement);

    /*** bind reading clause ***/
    std::unique_ptr<BoundReadingClause> bindReadingClause(
        const parser::ReadingClause& readingClause);
    std::unique_ptr<BoundReadingClause> bindMatchClause(const parser::ReadingClause& readingClause);
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

    std::vector<BoundInsertInfo> bindInsertInfos(
        const QueryGraphCollection& queryGraphCollection, const expression_set& nodeRelScope_);
    void bindInsertNode(std::shared_ptr<NodeExpression> node, std::vector<BoundInsertInfo>& infos);
    void bindInsertRel(std::shared_ptr<RelExpression> rel, std::vector<BoundInsertInfo>& infos);
    expression_vector bindInsertColumnDataExprs(
        const std::unordered_map<std::string, std::shared_ptr<Expression>>& propertyRhsExpr,
        const std::vector<catalog::Property>& properties);

    BoundSetPropertyInfo bindSetPropertyInfo(
        parser::ParsedExpression* lhs, parser::ParsedExpression* rhs);
    expression_pair bindSetItem(parser::ParsedExpression* lhs, parser::ParsedExpression* rhs);

    /*** bind projection clause ***/
    BoundWithClause bindWithClause(const parser::WithClause& withClause);
    BoundReturnClause bindReturnClause(const parser::ReturnClause& returnClause);
    BoundProjectionBody bindProjectionBody(const parser::ProjectionBody& projectionBody,
        const expression_vector& projectionExpressions);

    expression_vector bindProjectionExpressions(
        const parser::parsed_expr_vector& parsedExpressions);

    expression_vector bindOrderByExpressions(
        const std::vector<std::unique_ptr<parser::ParsedExpression>>& orderByExpressions);
    uint64_t bindSkipLimitExpression(const parser::ParsedExpression& expression);

    void addExpressionsToScope(const expression_vector& projectionExpressions);
    void resolveAnyDataTypeWithDefaultType(const expression_vector& expressions);

    /*** bind graph pattern ***/
    BoundGraphPattern bindGraphPattern(const std::vector<parser::PatternElement>& graphPattern);

    QueryGraph bindPatternElement(const parser::PatternElement& patternElement);
    std::shared_ptr<Expression> createPath(
        const std::string& pathName, const expression_vector& children);

    std::shared_ptr<RelExpression> bindQueryRel(const parser::RelPattern& relPattern,
        const std::shared_ptr<NodeExpression>& leftNode,
        const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph);
    std::shared_ptr<RelExpression> createNonRecursiveQueryRel(const std::string& parsedName,
        const std::vector<common::table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType);
    std::shared_ptr<RelExpression> createRecursiveQueryRel(const parser::RelPattern& relPattern,
        const std::vector<common::table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
        std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType);
    std::pair<uint64_t, uint64_t> bindVariableLengthRelBound(const parser::RelPattern& relPattern);
    void bindQueryRelProperties(RelExpression& rel);

    std::shared_ptr<NodeExpression> bindQueryNode(
        const parser::NodePattern& nodePattern, QueryGraph& queryGraph);
    std::shared_ptr<NodeExpression> createQueryNode(const parser::NodePattern& nodePattern);
    std::shared_ptr<NodeExpression> createQueryNode(
        const std::string& parsedName, const std::vector<common::table_id_t>& tableIDs);
    void bindQueryNodeProperties(NodeExpression& node);

    /*** bind table ID ***/
    // Bind table names to catalog table schemas. The function does NOT validate if the table schema
    // type matches node or rel pattern.
    std::vector<common::table_id_t> bindTableIDs(
        const std::vector<std::string>& tableNames, bool nodePattern);
    std::vector<common::table_id_t> getNodeTableIDs(
        const std::vector<common::table_id_t>& tableIDs);
    std::vector<common::table_id_t> getNodeTableIDs(common::table_id_t tableID);
    std::vector<common::table_id_t> getRelTableIDs(const std::vector<common::table_id_t>& tableIDs);
    std::vector<common::table_id_t> getRelTableIDs(common::table_id_t tableID);

    /*** validations ***/
    // E.g. ... RETURN a, b AS a
    static void validateProjectionColumnNamesAreUnique(const expression_vector& expressions);

    // E.g. ... WITH COUNT(*) MATCH ...
    static void validateProjectionColumnsInWithClauseAreAliased(
        const expression_vector& expressions);

    static void validateOrderByFollowedBySkipOrLimitInWithClause(
        const BoundProjectionBody& boundProjectionBody);

    // We don't support read after write for simplicity. User should instead querying through
    // multiple statement.
    static void validateReadNotFollowUpdate(const NormalizedSingleQuery& singleQuery);

    void validateTableType(common::table_id_t tableID, common::TableType expectedTableType);
    void validateTableExist(const std::string& tableName);

    /*** helpers ***/
    std::string getUniqueExpressionName(const std::string& name);
    static bool isReservedPropertyName(const std::string& name);

    std::unique_ptr<BinderScope> saveScope();
    void restoreScope(std::unique_ptr<BinderScope> prevVariableScope);

    function::TableFunction* getScanFunction(
        common::FileType fileType, const common::ReaderConfig& config);

private:
    const catalog::Catalog& catalog;
    storage::MemoryManager* memoryManager;
    storage::StorageManager* storageManager;
    common::VirtualFileSystem* vfs;
    uint32_t lastExpressionId;
    std::unique_ptr<BinderScope> scope;
    ExpressionBinder expressionBinder;
    main::ClientContext* clientContext;
    extension::ExtensionOptions* extensionOptions;
};

} // namespace binder
} // namespace kuzu
