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
    TableCatalogEntry* tableEntry;

    ShowConnectionBindData(const ClientContext* context, TableCatalogEntry* tableEntry,
        binder::expression_vector columns, offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, context{context},
          tableEntry{tableEntry} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(context, tableEntry, columns, maxOffset);
    }
};

static void outputRelTableConnection(const DataChunk& outputDataChunk, uint64_t outputPos,
    const ClientContext* context, table_id_t tableID) {
    const auto catalog = context->getCatalog();
    const auto tableEntry = catalog->getTableCatalogEntry(context->getTransaction(), tableID);
    const auto relTableEntry = ku_dynamic_cast<RelTableCatalogEntry*>(tableEntry);
    KU_ASSERT(tableEntry->getTableType() == TableType::REL);
    // Get src and dst name
    const auto srcTableID = relTableEntry->getSrcTableID();
    const auto dstTableID = relTableEntry->getDstTableID();
    const auto srcTableName = catalog->getTableName(context->getTransaction(), srcTableID);
    const auto dstTableName = catalog->getTableName(context->getTransaction(), dstTableID);
    // Get src and dst primary key
    const auto srcTableEntry = catalog->getTableCatalogEntry(context->getTransaction(), srcTableID);
    const auto dstTableEntry = catalog->getTableCatalogEntry(context->getTransaction(), dstTableID);
    const auto srcTablePrimaryKey =
        srcTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    const auto dstTablePrimaryKey =
        dstTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    // Write result to dataChunk
    outputDataChunk.getValueVectorMutable(0).setValue(outputPos, srcTableName);
    outputDataChunk.getValueVectorMutable(1).setValue(outputPos, dstTableName);
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
    const auto tableEntry = bindData->tableEntry;
    const auto numRelationsToOutput = morsel.endOffset - morsel.startOffset;
    auto vectorPos = 0u;
    switch (tableEntry->getTableType()) {
    case TableType::REL: {
        outputRelTableConnection(dataChunk, vectorPos, bindData->context, tableEntry->getTableID());
        vectorPos++;
    } break;
    case TableType::REL_GROUP: {
        const auto relGroupEntry = ku_dynamic_cast<RelGroupCatalogEntry*>(tableEntry);
        const auto relTableIDs = relGroupEntry->getRelTableIDs();
        for (; vectorPos < numRelationsToOutput; vectorPos++) {
            outputRelTableConnection(dataChunk, vectorPos, bindData->context,
                relTableIDs[morsel.startOffset + vectorPos]);
        }
    } break;
    default:
        // LCOV_EXCL_START
        KU_UNREACHABLE;
        // LCOV_EXCL_STOP
    }
    return vectorPos;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    const auto tableName = input->getLiteralVal<std::string>(0);
    const auto catalog = context->getCatalog();
    const auto tableID = catalog->getTableID(context->getTransaction(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTransaction(), tableID);
    const auto tableType = tableEntry->getTableType();
    if (tableType != TableType::REL && tableType != TableType::REL_GROUP) {
        throw BinderException{"Show connection can only be called on a rel table!"};
    }
    columnNames.emplace_back("source table name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("destination table name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("source table primary key");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("destination table primary key");
    columnTypes.emplace_back(LogicalType::STRING());
    offset_t maxOffset = 1;
    if (tableEntry->getTableType() == TableType::REL_GROUP) {
        const auto relGroupEntry = ku_dynamic_cast<RelGroupCatalogEntry*>(tableEntry);
        maxOffset = relGroupEntry->getRelTableIDs().size();
    }
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowConnectionBindData>(context, tableEntry, columns, maxOffset);
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
