#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"
#include "storage/store/table.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct DBStatsBindData final : TableFuncBindData {
    std::vector<catalog::TableCatalogEntry*> tables;
    std::vector<catalog::RelGroupCatalogEntry*> relGroups;

    explicit DBStatsBindData(std::vector<catalog::TableCatalogEntry*> tables,
        std::vector<catalog::RelGroupCatalogEntry*> relGroups, binder::expression_vector columns,
        row_idx_t numRows)
        : TableFuncBindData{std::move(columns), numRows}, tables{std::move(tables)},
          relGroups{std::move(relGroups)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DBStatsBindData>(tables, relGroups, columns, numRows);
    }
};

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("table_name");
    returnColumnNames.emplace_back("num_tuples");
    returnTypes.emplace_back(LogicalType::STRING());
    returnTypes.emplace_back(LogicalType::UINT64());
    returnColumnNames =
        TableFunction::extractYieldVariables(returnColumnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    std::vector<catalog::TableCatalogEntry*> tables;
    std::vector<catalog::RelGroupCatalogEntry*> relGroups;
    if (!context->hasDefaultDatabase()) {
        for (auto& entry :
            catalog->getTableEntries(transaction, context->useInternalCatalogEntry())) {
            if (entry->getType() == catalog::CatalogEntryType::REL_TABLE_ENTRY &&
                entry->constCast<catalog::RelTableCatalogEntry>().hasParentRelGroup(catalog,
                    transaction)) {
                continue;
            }
            tables.emplace_back(entry);
        }
        for (auto& entry : catalog->getRelGroupEntries(transaction)) {
            relGroups.emplace_back(entry);
        }
    }
    return std::make_unique<DBStatsBindData>(tables, relGroups, std::move(columns),
        tables.size() + relGroups.size());
}

static offset_t tableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
    DataChunk& output) {
    auto transaction = input.context->clientContext->getTransaction();
    auto storageManager = input.context->clientContext->getStorageManager();
    auto bindData = input.bindData->constPtrCast<DBStatsBindData>();
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; ++i) {
        const auto tableIdx = morsel.startOffset + i;
        if (tableIdx < bindData->tables.size()) {
            const auto table = bindData->tables[tableIdx];
            output.getValueVectorMutable(0).setValue(i, table->getName());
            const auto numRows =
                storageManager->getTable(table->getTableID())->getNumRows(transaction);
            output.getValueVectorMutable(1).setValue<uint64_t>(i, numRows);
        } else {
            const auto relGroupIdx = tableIdx - bindData->tables.size();
            const auto relGroup = bindData->relGroups[relGroupIdx];
            output.getValueVectorMutable(0).setValue(i, relGroup->getName());
            auto numRows = 0u;
            for (auto& relTableID : relGroup->getRelTableIDs()) {
                const auto table = storageManager->getTable(relTableID);
                numRows += table->getNumRows(transaction);
            }
            output.getValueVectorMutable(1).setValue<uint64_t>(i, numRows);
        }
    }
    return numTablesToOutput;
}

function_set DBStatsFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = SimpleTableFunc::getTableFunc(tableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
