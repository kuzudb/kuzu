#include "function/drop_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "function/fts_bind_data.h"
#include "function/fts_utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"
#include "processor/execution_context.h"
#include "storage/index/index_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    const TableFuncBindInput* input) {
    storage::IndexUtils::validateAutoTransaction(*context, DropFTSFunction::name);
    auto indexName = input->getLiteralVal<std::string>(1);
    const auto tableEntry = storage::IndexUtils::bindTable(*context,
        input->getLiteralVal<std::string>(0), indexName, storage::IndexOperation::DROP);
    return std::make_unique<FTSBindData>(tableEntry->getName(), tableEntry->getTableID(), indexName,
        binder::expression_vector{});
}

std::string dropFTSIndexQuery(ClientContext& context, const TableFuncBindData& bindData) {
    context.setToUseInternalCatalogEntry();
    auto ftsBindData = bindData.constPtrCast<FTSBindData>();
    auto query = stringFormat("DROP TABLE `{}`;",
        FTSUtils::getAppearsInTableName(ftsBindData->tableID, ftsBindData->indexName));
    query += stringFormat("DROP TABLE `{}`;",
        FTSUtils::getDocsTableName(ftsBindData->tableID, ftsBindData->indexName));
    query += stringFormat("DROP TABLE `{}`;",
        FTSUtils::getTermsTableName(ftsBindData->tableID, ftsBindData->indexName));
    return query;
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto& ftsBindData = *input.bindData->constPtrCast<FTSBindData>();
    auto& context = *input.context;
    context.clientContext->getCatalog()->dropIndex(input.context->clientContext->getTransaction(),
        ftsBindData.tableID, ftsBindData.indexName);
    return 0;
}

function_set DropFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::STRING});
    func->tableFunc = tableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = TableFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = dropFTSIndexQuery;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
