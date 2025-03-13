#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "processor/execution_context.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct ShowConnectionBindData final : TableFuncBindData {
    std::vector<TableCatalogEntry*> entries;

    ShowConnectionBindData(std::vector<TableCatalogEntry*> entries,
        binder::expression_vector columns, offset_t maxOffset)
        : TableFuncBindData{std::move(columns), maxOffset}, entries{std::move(entries)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(entries, columns, numRows);
    }
};

static void outputRelTableConnection(DataChunk& outputDataChunk, uint64_t outputPos,
    const ClientContext& context, TableCatalogEntry* entry) {
    const auto catalog = context.getCatalog();
    KU_ASSERT(entry->getType() == CatalogEntryType::REL_TABLE_ENTRY);
    const auto relTableEntry = ku_dynamic_cast<RelTableCatalogEntry*>(entry);

    // Get src and dst name
    const auto srcTableID = relTableEntry->getSrcTableID();
    const auto dstTableID = relTableEntry->getDstTableID();
    // Get src and dst primary key
    const auto srcTableEntry = catalog->getTableCatalogEntry(context.getTransaction(), srcTableID);
    const auto dstTableEntry = catalog->getTableCatalogEntry(context.getTransaction(), dstTableID);
    const auto srcTablePrimaryKey =
        srcTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    const auto dstTablePrimaryKey =
        dstTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    // Write result to dataChunk
    outputDataChunk.getValueVectorMutable(0).setValue(outputPos, srcTableEntry->getName());
    outputDataChunk.getValueVectorMutable(1).setValue(outputPos, dstTableEntry->getName());
    outputDataChunk.getValueVectorMutable(2).setValue(outputPos, srcTablePrimaryKey);
    outputDataChunk.getValueVectorMutable(3).setValue(outputPos, dstTablePrimaryKey);
}

static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
    DataChunk& output) {
    const auto bindData = input.bindData->constPtrCast<ShowConnectionBindData>();
    auto i = 0u;
    auto size = morsel.getMorselSize();
    for (; i < size; i++) {
        outputRelTableConnection(output, i, *input.context->clientContext,
            bindData->entries[i + morsel.startOffset]);
    }
    return i;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("source table name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("destination table name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("source table primary key");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("destination table primary key");
    columnTypes.emplace_back(LogicalType::STRING());
    const auto name = input->getLiteralVal<std::string>(0);
    const auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    std::vector<TableCatalogEntry*> entries;
    if (catalog->containsTable(transaction, name)) {
        auto entry = catalog->getTableCatalogEntry(transaction, name);
        if (entry->getType() != catalog::CatalogEntryType::REL_TABLE_ENTRY) {
            throw BinderException{"Show connection can only be called on a rel table!"};
        }
        entries.push_back(entry);
    } else if (catalog->containsRelGroup(transaction, name)) {
        auto entry = catalog->getRelGroupEntry(transaction, name);
        for (auto& id : entry->getRelTableIDs()) {
            entries.push_back(catalog->getTableCatalogEntry(transaction, id));
        }
    } else {
        throw BinderException{"Show connection can only be called on a rel table!"};
    }
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowConnectionBindData>(entries, columns, entries.size());
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
