#include "binder/binder.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/simple_table_functions.h"
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

struct StatsInfoBindData final : SimpleTableFuncBindData {
    TableCatalogEntry* tableEntry;
    storage::Table* table;
    ClientContext* context;

    StatsInfoBindData(binder::expression_vector columns, TableCatalogEntry* tableEntry,
        storage::Table* table, ClientContext* context)
        : SimpleTableFuncBindData{std::move(columns), 1 /*maxOffset*/}, tableEntry{tableEntry},
          table{table}, context{context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<StatsInfoBindData>(columns, tableEntry, table, context);
    }
};

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    const auto& dataChunk = output.dataChunk;
    KU_ASSERT(dataChunk.state->getSelVector().isUnfiltered());
    const auto morsel = input.sharedState->ptrCast<SimpleTableFuncSharedState>()->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    const auto bindData = input.bindData->constPtrCast<StatsInfoBindData>();
    const auto table = bindData->table;
    switch (table->getTableType()) {
    case TableType::NODE: {
        const auto& nodeTable = table->cast<storage::NodeTable>();
        const auto stats = nodeTable.getStats(bindData->context->getTx());
        dataChunk.getValueVectorMutable(0).setValue<cardinality_t>(0, stats.getTableCard());
        for (auto i = 0u; i < nodeTable.getNumColumns(); ++i) {
            dataChunk.getValueVectorMutable(i + 1).setValue(0, stats.getNumDistinctValues(i));
        }
        dataChunk.state->getSelVectorUnsafe().setToUnfiltered(1);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    TableFuncBindInput* input) {
    const auto tableName = input->getLiteralVal<std::string>(0);
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

    std::vector<std::string> columnNames = {"cardinality"};
    std::vector<LogicalType> columnTypes;
    columnTypes.push_back(LogicalType::INT64());
    for (auto& propDef : tableEntry->getProperties()) {
        columnNames.push_back(propDef.getName() + "_distinct_count");
        columnTypes.push_back(LogicalType::INT64());
    }
    const auto storageManager = context->getStorageManager();
    auto table = storageManager->getTable(tableID);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<StatsInfoBindData>(columns, tableEntry, table, context);
}

function_set StatsInfoFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
