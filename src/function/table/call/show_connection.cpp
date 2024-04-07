#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/bind_input.h"
#include "function/table/call_functions.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct ShowConnectionBindData : public CallTableFuncBindData {
    ClientContext* context;
    TableCatalogEntry* tableEntry;

    ShowConnectionBindData(ClientContext* context, TableCatalogEntry* tableEntry,
        std::vector<LogicalType> columnTypes, std::vector<std::string> columnNames,
        offset_t maxOffset)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames), maxOffset},
          context{context}, tableEntry{tableEntry} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(context, tableEntry, columnTypes,
            columnNames, maxOffset);
    }
};

static void outputRelTableConnection(ValueVector* srcTableNameVector,
    ValueVector* dstTableNameVector, uint64_t outputPos, ClientContext* context,
    table_id_t tableID) {
    auto catalog = context->getCatalog();
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry);
    KU_ASSERT(tableEntry->getTableType() == TableType::REL);
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();
    srcTableNameVector->setValue(outputPos, catalog->getTableName(context->getTx(), srcTableID));
    dstTableNameVector->setValue(outputPos, catalog->getTableName(context->getTx(), dstTableID));
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto srcVector = dataChunk.getValueVector(0).get();
    auto dstVector = dataChunk.getValueVector(1).get();
    auto morsel = ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState)
                      ->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto bindData =
        ku_dynamic_cast<function::TableFuncBindData*, function::ShowConnectionBindData*>(
            input.bindData);
    auto tableEntry = bindData->tableEntry;
    auto numRelationsToOutput = morsel.endOffset - morsel.startOffset;
    auto vectorPos = 0u;
    switch (tableEntry->getTableType()) {
    case TableType::REL: {
        outputRelTableConnection(srcVector, dstVector, vectorPos, bindData->context,
            tableEntry->getTableID());
        vectorPos++;
    } break;
    case TableType::REL_GROUP: {
        auto relGroupEntry = ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        auto relTableIDs = relGroupEntry->getRelTableIDs();
        for (; vectorPos < numRelationsToOutput; vectorPos++) {
            outputRelTableConnection(srcVector, dstVector, vectorPos, bindData->context,
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
    // Special case here Due to any -> string, but lack implicit cast
    if (input->inputs[0].getDataType()->getLogicalTypeID() != LogicalTypeID::STRING) {
        throw BinderException{"Show connection can only bind to String!"};
    }
    auto tableName = input->inputs[0].getValue<std::string>();
    auto catalog = context->getCatalog();
    auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto tableType = tableEntry->getTableType();
    if (tableType != TableType::REL && tableType != TableType::REL_GROUP) {
        throw BinderException{"Show connection can only be called on a rel table!"};
    }
    columnNames.emplace_back("source table name");
    columnTypes.emplace_back(*LogicalType::STRING());
    columnNames.emplace_back("destination table name");
    columnTypes.emplace_back(*LogicalType::STRING());
    common::offset_t maxOffset = 1;
    if (tableEntry->getTableType() == common::TableType::REL_GROUP) {
        auto relGroupEntry = ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        maxOffset = relGroupEntry->getRelTableIDs().size();
    }
    return std::make_unique<ShowConnectionBindData>(context, tableEntry, std::move(columnTypes),
        std::move(columnNames), maxOffset);
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("db_version", tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
