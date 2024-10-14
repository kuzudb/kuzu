#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/call_functions.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput&,
    TableFuncSharedState*, storage::MemoryManager*) {
    return std::make_unique<TableFuncLocalState>();
}

struct StatsInfoBindData final : CallTableFuncBindData {
    TableCatalogEntry* tableEntry;
    storage::Table* table;
    ClientContext* context;

    StatsInfoBindData(std::vector<LogicalType> columnTypes, std::vector<std::string> columnNames,
        TableCatalogEntry* tableEntry, storage::Table* table, ClientContext* context)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames), 1 /*maxOffset*/},
          tableEntry{tableEntry}, table{table}, context{context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<StatsInfoBindData>(LogicalType::copy(columnTypes), columnNames,
            tableEntry, table, context);
    }
};

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    const auto& dataChunk = output.dataChunk;
    KU_ASSERT(dataChunk.state->getSelVector().isUnfiltered());
    const auto morsel = input.sharedState->ptrCast<CallFuncSharedState>()->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<StatsInfoBindData>();
    const auto table = bindData->table;
    switch (table->getTableType()) {
    case TableType::NODE: {
        const auto& nodeTable = table->cast<storage::NodeTable>();
        const auto stats = nodeTable.getStats(bindData->context->getTx());
        dataChunk.getValueVectorMutable(0).setValue<cardinality_t>(0, stats.getCardinality());
        dataChunk.state->getSelVectorUnsafe().setToUnfiltered(1);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    ScanTableFuncBindInput* input) {
    std::vector<std::string> columnNames = {"cardinality"};
    std::vector<LogicalType> columnTypes;
    columnTypes.push_back(LogicalType::INT64());
    const auto tableName = input->inputs[0].getValue<std::string>();
    const auto catalog = context->getCatalog();
    if (!catalog->containsTable(context->getTx(), tableName)) {
        throw BinderException{"Table " + tableName + " does not exist!"};
    }
    const auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    if (tableEntry->getTableType() != TableType::NODE) {
        throw BinderException{
            "Stats from a non-node table " + tableName + " is not supported yet!"};
    }
    const auto storageManager = context->getStorageManager();
    auto table = storageManager->getTable(tableID);
    return std::make_unique<StatsInfoBindData>(std::move(columnTypes), std::move(columnNames),
        tableEntry, table, context);
}

function_set StatsInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
