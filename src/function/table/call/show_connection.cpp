#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_functions.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct ShowConnectionBindData final : SimpleTableFuncBindData {
    const ClientContext* context;
    std::vector<TableCatalogEntry*> entries;

    ShowConnectionBindData(const ClientContext* context, std::vector<TableCatalogEntry*> entries,
        binder::expression_vector columns, offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, context{context},
          entries{std::move(entries)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(context, entries, columns, maxOffset);
    }
};

static void outputRelTableConnection(const DataChunk& outputDataChunk, uint64_t outputPos,
    const ClientContext* context, TableCatalogEntry* entry) {
    const auto catalog = context->getCatalog();
    KU_ASSERT(entry->getType() == CatalogEntryType::REL_TABLE_ENTRY);
    const auto relTableEntry = ku_dynamic_cast<RelTableCatalogEntry*>(entry);

    // Get src and dst name
    const auto srcTableID = relTableEntry->getSrcTableID();
    const auto dstTableID = relTableEntry->getDstTableID();
    // Get src and dst primary key
    const auto srcTableEntry = catalog->getTableCatalogEntry(context->getTransaction(), srcTableID);
    const auto dstTableEntry = catalog->getTableCatalogEntry(context->getTransaction(), dstTableID);
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

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto morsel = input.sharedState->ptrCast<SimpleTableFuncSharedState>()->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<ShowConnectionBindData>();
    auto vectorPos = 0u;
    for (auto& entry : bindData->entries) {
        outputRelTableConnection(dataChunk, vectorPos, bindData->context, entry);
        vectorPos++;
    }
    return vectorPos;
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
    offset_t maxOffset = 1;
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
    maxOffset = entries.size();

    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowConnectionBindData>(context, entries, columns, maxOffset);
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
