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

struct ShowConnectionBindData : public SimpleTableFuncBindData {
    ClientContext* context;
    TableCatalogEntry* tableEntry;

    ShowConnectionBindData(ClientContext* context, TableCatalogEntry* tableEntry,
        binder::expression_vector columns, offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, context{context},
          tableEntry{tableEntry} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(context, tableEntry, columns, maxOffset);
    }
};

static void outputRelTableConnection(DataChunk& outputDataChunk, uint64_t outputPos,
    ClientContext* context, table_id_t tableID) {
    auto catalog = context->getCatalog();
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto relTableEntry = ku_dynamic_cast<RelTableCatalogEntry*>(tableEntry);
    KU_ASSERT(tableEntry->getTableType() == TableType::REL);
    // Get src and dst name
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();
    auto srcTableName = catalog->getTableName(context->getTx(), srcTableID);
    auto dstTableName = catalog->getTableName(context->getTx(), dstTableID);
    // Get src and dst primary key
    auto srcTableEntry = catalog->getTableCatalogEntry(context->getTx(), srcTableID);
    auto dstTableEntry = catalog->getTableCatalogEntry(context->getTx(), dstTableID);
    auto srcTablePrimaryKey = srcTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    auto dstTablePrimaryKey = dstTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    // Write result to dataChunk
    outputDataChunk.getValueVectorMutable(0).setValue(outputPos, srcTableName);
    outputDataChunk.getValueVectorMutable(1).setValue(outputPos, dstTableName);
    outputDataChunk.getValueVectorMutable(2).setValue(outputPos, srcTablePrimaryKey);
    outputDataChunk.getValueVectorMutable(3).setValue(outputPos, dstTablePrimaryKey);
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto morsel = input.sharedState->ptrCast<SimpleTableFuncSharedState>()->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto bindData = input.bindData->constPtrCast<ShowConnectionBindData>();
    auto tableEntry = bindData->tableEntry;
    auto numRelationsToOutput = morsel.endOffset - morsel.startOffset;
    auto vectorPos = 0u;
    switch (tableEntry->getTableType()) {
    case TableType::REL: {
        outputRelTableConnection(dataChunk, vectorPos, bindData->context, tableEntry->getTableID());
        vectorPos++;
    } break;
    case TableType::REL_GROUP: {
        auto relGroupEntry = ku_dynamic_cast<RelGroupCatalogEntry*>(tableEntry);
        auto relTableIDs = relGroupEntry->getRelTableIDs();
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

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    auto tableName = input->getLiteralVal<std::string>(0);
    auto catalog = context->getCatalog();
    auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto tableType = tableEntry->getTableType();
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
    common::offset_t maxOffset = 1;
    if (tableEntry->getTableType() == common::TableType::REL_GROUP) {
        auto relGroupEntry = ku_dynamic_cast<RelGroupCatalogEntry*>(tableEntry);
        maxOffset = relGroupEntry->getRelTableIDs().size();
    }
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowConnectionBindData>(context, tableEntry, columns, maxOffset);
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = initSharedState;
    function->initLocalStateFunc = initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
