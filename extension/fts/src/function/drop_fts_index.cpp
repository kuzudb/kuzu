#include "function/drop_fts_index.h"

#include "function/fts_bind_data.h"
#include "function/fts_index_utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    const TableFuncBindInput* input) {
    FTSIndexUtils::validateAutoTransaction(*context, DropFTSFunction::name);
    auto indexName = input->getLiteralVal<std::string>(1);
    const auto tableEntry = FTSIndexUtils::bindNodeTable(*context,
        input->getLiteralVal<std::string>(0), indexName, FTSIndexUtils::IndexOperation::DROP);
    return std::make_unique<FTSBindData>(tableEntry->getName(), tableEntry->getTableID(), indexName,
        binder::expression_vector{});
}

std::string dropFTSIndexQuery(ClientContext& context, const TableFuncBindData& bindData) {
    context.setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    auto ftsBindData = bindData.constPtrCast<FTSBindData>();
    auto query = stringFormat("CALL _DROP_FTS_INDEX('{}', '{}');", ftsBindData->tableName,
        ftsBindData->indexName);
    query += stringFormat("DROP TABLE `{}`;",
        FTSUtils::getAppearsInTableName(ftsBindData->tableID, ftsBindData->indexName));
    query += stringFormat("DROP TABLE `{}`;",
        FTSUtils::getDocsTableName(ftsBindData->tableID, ftsBindData->indexName));
    query += stringFormat("DROP TABLE `{}`;",
        FTSUtils::getTermsTableName(ftsBindData->tableID, ftsBindData->indexName));
    return query;
}

static offset_t internalTableFunc(const TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto& ftsBindData = *input.bindData->constPtrCast<FTSBindData>();
    auto& context = *input.context;
    context.clientContext->getCatalog()->dropIndex(input.context->clientContext->getTransaction(),
        ftsBindData.tableID, ftsBindData.indexName);
    context.clientContext->getStorageManager()
        ->getTable(ftsBindData.tableID)
        ->cast<storage::NodeTable>()
        .dropIndex(ftsBindData.indexName);
    return 0;
}

function_set InternalDropFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::STRING});
    func->tableFunc = internalTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

function_set DropFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::STRING});
    func->tableFunc = TableFunction::emptyTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = dropFTSIndexQuery;
    func->canParallelFunc = [] { return false; };
    func->isReadOnly = false;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
