#include "function/drop_fts_index.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "fts_extension.h"
#include "function/fts_bind_data.h"
#include "function/fts_utils.h"
#include "function/table/bind_input.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    TableFuncBindInput* input) {
    FTSUtils::validateAutoTrx(*context, DropFTSFunction::name);
    auto indexName = input->getLiteralVal<std::string>(1);
    auto& tableEntry = FTSUtils::bindTable(input->getLiteralVal<std::string>(0), context, indexName,
        FTSUtils::IndexOperation::DROP);
    FTSUtils::validateIndexExistence(*context, tableEntry.getTableID(), indexName);
    return std::make_unique<FTSBindData>(tableEntry.getName(), tableEntry.getTableID(), indexName,
        std::vector<common::LogicalType>{}, std::vector<std::string>{});
}

std::string dropFTSIndexQuery(ClientContext& /*context*/, const TableFuncBindData& bindData) {
    auto ftsBindData = bindData.constPtrCast<FTSBindData>();
    std::string query =
        common::stringFormat("DROP TABLE `{}`;", ftsBindData->getStopWordsTableName());
    query += common::stringFormat("DROP TABLE `{}`;", ftsBindData->getAppearsInTableName());
    query += common::stringFormat("DROP TABLE `{}`;", ftsBindData->getDocsTableName());
    query += common::stringFormat("DROP TABLE `{}`;", ftsBindData->getTermsTableName());
    return query;
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto& ftsBindData = *input.bindData->constPtrCast<FTSBindData>();
    auto& context = *input.context;
    context.clientContext->getCatalog()->dropIndex(input.context->clientContext->getTx(),
        ftsBindData.tableID, ftsBindData.indexName);
    return 0;
}

function_set DropFTSFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState,
        initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING});
    func->rewriteFunc = dropFTSIndexQuery;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace fts_extension
} // namespace kuzu
