#include "function/table_functions/call_functions.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/keyword/rdf_keyword.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include "storage/store/var_list_column.h"
#include "transaction/transaction.h"

using namespace kuzu::transaction;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::main;
using namespace kuzu::storage;

namespace kuzu {
namespace function {

std::unique_ptr<TableFuncLocalState> initLocalState(
    TableFunctionInitInput& /*input*/, TableFuncSharedState* /*state*/, MemoryManager* /*mm*/) {
    return std::make_unique<TableFuncLocalState>();
}

function_set CurrentSettingFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("current_setting", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

CallFuncMorsel CallFuncSharedState::getMorsel() {
    std::lock_guard lck{mtx};
    KU_ASSERT(curOffset <= maxOffset);
    if (curOffset == maxOffset) {
        return CallFuncMorsel::createInvalidMorsel();
    } else {
        auto numValuesToOutput = std::min(DEFAULT_VECTOR_CAPACITY, maxOffset - curOffset);
        curOffset += numValuesToOutput;
        return CallFuncMorsel{curOffset - numValuesToOutput, curOffset};
    }
}

std::unique_ptr<TableFuncSharedState> CallFunction::initSharedState(TableFunctionInitInput& input) {
    auto callTableFuncBindData =
        ku_dynamic_cast<TableFuncBindData*, CallTableFuncBindData*>(input.bindData);
    return std::make_unique<CallFuncSharedState>(callTableFuncBindData->maxOffset);
}

void CurrentSettingFunction::tableFunc(TableFunctionInput& data, DataChunk& output) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(data.sharedState);
    auto outputVector = output.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        output.state->selVector->selectedSize = 0;
        return;
    }
    auto currentSettingBindData =
        ku_dynamic_cast<TableFuncBindData*, CurrentSettingBindData*>(data.bindData);
    auto pos = output.state->selVector->selectedPositions[0];
    outputVector->setValue(pos, currentSettingBindData->result);
    outputVector->setNull(pos, false);
    output.state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> CurrentSettingFunction::bindFunc(ClientContext* context,
    TableFuncBindInput* input, Catalog* /*catalog*/, StorageManager* /*storageManager*/) {
    auto optionName = input->inputs[0].getValue<std::string>();
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back(optionName);
    returnTypes.push_back(*LogicalType::STRING());
    return std::make_unique<CurrentSettingBindData>(
        context->getCurrentSetting(optionName).toString(), std::move(returnTypes),
        std::move(returnColumnNames), 1 /* one row result */);
}

function_set DBVersionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("db_version", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

void DBVersionFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto outputVector = outputChunk.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        outputChunk.state->selVector->selectedSize = 0;
        return;
    }
    auto pos = outputChunk.state->selVector->selectedPositions[0];
    outputVector->setValue(pos, std::string(KUZU_VERSION));
    outputVector->setNull(pos, false);
    outputChunk.state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> DBVersionFunction::bindFunc(ClientContext* /*context*/,
    TableFuncBindInput* /*input*/, Catalog* /*catalog*/, StorageManager* /*storageManager*/) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(*LogicalType::STRING());
    return std::make_unique<CallTableFuncBindData>(
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

function_set ShowTablesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("show_tables", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

void ShowTablesFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.getValueVector(0)->state->selVector->selectedSize = 0;
        return;
    }
    auto tables =
        ku_dynamic_cast<function::TableFuncBindData*, function::ShowTablesBindData*>(input.bindData)
            ->tables;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto tableEntry = tables[morsel.startOffset + i];
        outputChunk.getValueVector(0)->setValue(i, tableEntry->getName());
        std::string typeString = TableTypeUtils::toString(tableEntry->getTableType());
        outputChunk.getValueVector(1)->setValue(i, typeString);
        outputChunk.getValueVector(2)->setValue(i, tableEntry->getComment());
    }
    outputChunk.state->selVector->selectedSize = numTablesToOutput;
}

std::unique_ptr<TableFuncBindData> ShowTablesFunction::bindFunc(ClientContext* context,
    TableFuncBindInput* /*input*/, Catalog* catalog, StorageManager* /*storageManager*/) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(*LogicalType::STRING());
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(*LogicalType::STRING());
    returnColumnNames.emplace_back("comment");
    returnTypes.emplace_back(*LogicalType::STRING());
    return std::make_unique<ShowTablesBindData>(catalog->getTableEntries(context->getTx()),
        std::move(returnTypes), std::move(returnColumnNames),
        catalog->getTableCount(context->getTx()));
}

function_set TableInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("table_info", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void TableInfoFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.getValueVector(0)->state->selVector->selectedSize = 0;
        return;
    }
    auto bindData =
        ku_dynamic_cast<function::TableFuncBindData*, function::TableInfoBindData*>(input.bindData);
    auto tableEntry = bindData->tableEntry;
    auto numPropertiesToOutput = morsel.endOffset - morsel.startOffset;
    auto vectorPos = 0;
    for (auto i = 0u; i < numPropertiesToOutput; i++) {
        auto property = &tableEntry->getPropertiesRef()[morsel.startOffset + i];
        if (tableEntry->getTableType() == TableType::REL) {
            if (property->getName() == InternalKeyword::ID) {
                // Skip internal id column.
                continue;
            }
            if (property->getName() == rdf::PID) {
                // Replace pid column with (virtual) iri column.
                outputChunk.getValueVector(0)->setValue<int64_t>(vectorPos, -1);
                outputChunk.getValueVector(1)->setValue(
                    vectorPos, std::string(rdf::IRI) + " (Virtual)");
                outputChunk.getValueVector(2)->setValue(
                    vectorPos, LogicalType::STRING()->toString());
                vectorPos++;
                continue;
            }
        }
        outputChunk.getValueVector(0)->setValue(vectorPos, (int64_t)property->getPropertyID());
        outputChunk.getValueVector(1)->setValue(vectorPos, property->getName());
        outputChunk.getValueVector(2)->setValue(vectorPos, property->getDataType()->toString());

        if (tableEntry->getTableType() == TableType::NODE) {
            auto nodeTableEntry =
                ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableEntry);
            auto primaryKeyID = nodeTableEntry->getPrimaryKeyPID();
            outputChunk.getValueVector(3)->setValue(
                vectorPos, primaryKeyID == property->getPropertyID());
        }
        vectorPos++;
    }
    outputChunk.state->selVector->selectedSize = vectorPos;
}

std::unique_ptr<TableFuncBindData> TableInfoFunction::bindFunc(ClientContext* context,
    TableFuncBindInput* input, Catalog* catalog, StorageManager* /*storageManager*/) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    auto tableName = input->inputs[0].getValue<std::string>();
    auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    returnColumnNames.emplace_back("property id");
    returnTypes.push_back(*LogicalType::INT64());
    returnColumnNames.emplace_back("name");
    returnTypes.push_back(*LogicalType::STRING());
    returnColumnNames.emplace_back("type");
    returnTypes.push_back(*LogicalType::STRING());
    if (tableEntry->getTableType() == TableType::NODE) {
        returnColumnNames.emplace_back("primary key");
        returnTypes.push_back(*LogicalType::BOOL());
    }
    return std::make_unique<TableInfoBindData>(tableEntry, std::move(returnTypes),
        std::move(returnColumnNames), tableEntry->getNumProperties());
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("db_version", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void ShowConnectionFunction::outputRelTableConnection(ValueVector* srcTableNameVector,
    ValueVector* dstTableNameVector, uint64_t outputPos, Catalog* catalog, table_id_t tableID) {
    auto tableEntry = catalog->getTableCatalogEntry(&DUMMY_READ_TRANSACTION, tableID);
    auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry);
    KU_ASSERT(tableEntry->getTableType() == TableType::REL);
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();
    srcTableNameVector->setValue(
        outputPos, catalog->getTableName(&DUMMY_READ_TRANSACTION, srcTableID));
    dstTableNameVector->setValue(
        outputPos, catalog->getTableName(&DUMMY_READ_TRANSACTION, dstTableID));
}

void ShowConnectionFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto morsel = ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState)
                      ->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.state->selVector->selectedSize = 0;
        return;
    }
    auto showConnectionBindData =
        ku_dynamic_cast<function::TableFuncBindData*, function::ShowConnectionBindData*>(
            input.bindData);
    auto tableEntry = showConnectionBindData->tableEntry;
    auto numRelationsToOutput = morsel.endOffset - morsel.startOffset;
    auto catalog = showConnectionBindData->catalog;
    auto vectorPos = 0u;
    switch (tableEntry->getTableType()) {
    case TableType::REL: {
        outputRelTableConnection(outputChunk.getValueVector(0).get(),
            outputChunk.getValueVector(1).get(), vectorPos, catalog, tableEntry->getTableID());
        vectorPos++;
    } break;
    case TableType::REL_GROUP: {
        auto relGroupEntry = ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(tableEntry);
        auto relTableIDs = relGroupEntry->getRelTableIDs();
        for (; vectorPos < numRelationsToOutput; vectorPos++) {
            outputRelTableConnection(outputChunk.getValueVector(0).get(),
                outputChunk.getValueVector(1).get(), vectorPos, catalog,
                relTableIDs[morsel.startOffset + vectorPos]);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    outputChunk.state->selVector->selectedSize = vectorPos;
}

std::unique_ptr<TableFuncBindData> ShowConnectionFunction::bindFunc(ClientContext* context,
    TableFuncBindInput* input, Catalog* catalog, StorageManager* /*storageManager*/) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    // Special case here Due to any -> string, but lack implicit cast
    if (input->inputs[0].getDataType()->getLogicalTypeID() != LogicalTypeID::STRING) {
        throw BinderException{"Show connection can only bind to String!"};
    }
    auto tableName = input->inputs[0].getValue<std::string>();
    auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto tableType = tableEntry->getTableType();
    if (tableType != TableType::REL && tableType != TableType::REL_GROUP) {
        throw BinderException{"Show connection can only be called on a rel table!"};
    }
    returnColumnNames.emplace_back("source table name");
    returnTypes.emplace_back(*LogicalType::STRING());
    returnColumnNames.emplace_back("destination table name");
    returnTypes.emplace_back(*LogicalType::STRING());
    return std::make_unique<ShowConnectionBindData>(catalog, tableEntry, std::move(returnTypes),
        std::move(returnColumnNames),
        tableEntry->getTableType() == TableType::REL ?
            1 :
            ku_dynamic_cast<TableCatalogEntry*, RelGroupCatalogEntry*>(tableEntry)
                ->getRelTableIDs()
                .size());
}

std::unique_ptr<TableFuncBindData> StorageInfoFunction::bindFunc(ClientContext* context,
    TableFuncBindInput* input, Catalog* catalog, StorageManager* storageManager) {
    std::vector<std::string> columnNames = {"node_group_id", "column_name", "data_type",
        "table_type", "start_page_idx", "num_pages", "num_values", "compression"};
    std::vector<LogicalType> columnTypes;
    columnTypes.emplace_back(*LogicalType::INT64());
    columnTypes.emplace_back(*LogicalType::STRING());
    columnTypes.emplace_back(*LogicalType::STRING());
    columnTypes.emplace_back(*LogicalType::STRING());
    columnTypes.emplace_back(*LogicalType::INT64());
    columnTypes.emplace_back(*LogicalType::INT64());
    columnTypes.emplace_back(*LogicalType::INT64());
    columnTypes.emplace_back(*LogicalType::STRING());
    auto tableName = input->inputs[0].getValue<std::string>();
    if (!catalog->containsTable(context->getTx(), tableName)) {
        throw BinderException{"Table " + tableName + " does not exist!"};
    }
    auto tableID = catalog->getTableID(context->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(context->getTx(), tableID);
    auto table = tableEntry->getTableType() == TableType::NODE ?
                     reinterpret_cast<Table*>(storageManager->getNodeTable(tableID)) :
                     reinterpret_cast<Table*>(storageManager->getRelTable(tableID));
    return std::make_unique<StorageInfoBindData>(
        tableEntry, std::move(columnTypes), std::move(columnNames), table);
}

StorageInfoSharedState::StorageInfoSharedState(Table* table, offset_t maxOffset)
    : CallFuncSharedState{maxOffset} {
    collectColumns(table);
}

void StorageInfoSharedState::collectColumns(Table* table) {
    switch (table->getTableType()) {
    case TableType::NODE: {
        auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(table);
        for (auto columnID = 0u; columnID < nodeTable->getNumColumns(); columnID++) {
            auto collectedColumns = collectColumns(nodeTable->getColumn(columnID));
            columns.insert(columns.end(), collectedColumns.begin(), collectedColumns.end());
        }
    } break;
    case TableType::REL: {
        auto relTable = ku_dynamic_cast<Table*, RelTable*>(table);
        if (relTable->getTableDataFormat(RelDataDirection::FWD) == ColumnDataFormat::CSR) {
            columns.push_back(relTable->getCSROffsetColumn(RelDataDirection::FWD));
            columns.push_back(relTable->getCSRLengthColumn(RelDataDirection::FWD));
        }
        if (relTable->getTableDataFormat(RelDataDirection::BWD) == ColumnDataFormat::CSR) {
            columns.push_back(relTable->getCSROffsetColumn(RelDataDirection::BWD));
            columns.push_back(relTable->getCSRLengthColumn(RelDataDirection::BWD));
        }
        columns.push_back(relTable->getAdjColumn(RelDataDirection::FWD));
        columns.push_back(relTable->getAdjColumn(RelDataDirection::BWD));
        for (auto columnID = 0u; columnID < relTable->getNumColumns(); columnID++) {
            auto column = relTable->getColumn(columnID, RelDataDirection::FWD);
            auto collectedColumns = collectColumns(column);
            columns.insert(columns.end(), collectedColumns.begin(), collectedColumns.end());
            column = relTable->getColumn(columnID, RelDataDirection::BWD);
            collectedColumns = collectColumns(column);
            columns.insert(columns.end(), collectedColumns.begin(), collectedColumns.end());
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

std::vector<Column*> StorageInfoSharedState::collectColumns(Column* column) {
    std::vector<Column*> result;
    result.push_back(column);
    if (column->getNullColumn()) {
        result.push_back(column->getNullColumn());
    }
    switch (column->getDataType().getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        auto structColumn = ku_dynamic_cast<Column*, StructColumn*>(column);
        auto numChildren = StructType::getNumFields(&structColumn->getDataType());
        for (auto i = 0u; i < numChildren; i++) {
            auto childColumn = structColumn->getChild(i);
            auto subColumns = collectColumns(childColumn);
            result.insert(result.end(), subColumns.begin(), subColumns.end());
        }
    } break;
    case PhysicalTypeID::STRING: {
        auto stringColumn = ku_dynamic_cast<Column*, StringColumn*>(column);
        auto& dictionary = stringColumn->getDictionary();
        result.push_back(dictionary.getDataColumn());
        result.push_back(dictionary.getOffsetColumn());
    } break;
    case PhysicalTypeID::VAR_LIST: {
        auto varListColumn = ku_dynamic_cast<Column*, VarListColumn*>(column);
        result.push_back(varListColumn->getDataColumn());
    } break;
    default: {
        // DO NOTHING.
    }
    }
    return result;
}

std::unique_ptr<TableFuncSharedState> StorageInfoFunction::initSharedState(
    TableFunctionInitInput& input) {
    auto storageInfoBindData =
        ku_dynamic_cast<TableFuncBindData*, StorageInfoBindData*>(input.bindData);
    return std::make_unique<StorageInfoSharedState>(
        storageInfoBindData->table, storageInfoBindData->maxOffset);
}

std::unique_ptr<TableFuncLocalState> StorageInfoFunction::initLocalState(
    TableFunctionInitInput& /*input*/, TableFuncSharedState* /*sharedState*/, MemoryManager* mm) {
    return std::make_unique<StorageInfoLocalState>(mm);
}

void StorageInfoFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto localState =
        ku_dynamic_cast<TableFuncLocalState*, StorageInfoLocalState*>(input.localState);
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, StorageInfoSharedState*>(input.sharedState);
    KU_ASSERT(outputChunk.state->selVector->isUnfiltered());
    while (true) {
        if (localState->currChunkIdx < localState->dataChunkCollection->getNumChunks()) {
            // Copy from local state chunk.
            auto chunk = localState->dataChunkCollection->getChunk(localState->currChunkIdx);
            auto numValuesToOutput = chunk->state->selVector->selectedSize;
            for (auto columnIdx = 0u; columnIdx < outputChunk.getNumValueVectors(); columnIdx++) {
                auto localVector = chunk->getValueVector(columnIdx);
                auto outputVector = outputChunk.getValueVector(columnIdx);
                for (auto i = 0u; i < numValuesToOutput; i++) {
                    outputVector->copyFromVectorData(i, localVector.get(), i);
                }
            }
            outputChunk.state->selVector->resetSelectorToUnselectedWithSize(numValuesToOutput);
            localState->currChunkIdx++;
            return;
        }
        auto morsel =
            ku_dynamic_cast<TableFuncSharedState*, CallFuncSharedState*>(input.sharedState)
                ->getMorsel();
        if (!morsel.hasMoreToOutput()) {
            outputChunk.state->selVector->selectedSize = 0;
            return;
        }
        auto storageInfoBindData =
            ku_dynamic_cast<TableFuncBindData*, StorageInfoBindData*>(input.bindData);
        auto tableEntry = storageInfoBindData->tableEntry;
        std::string tableType = tableEntry->getTableType() == TableType::NODE ? "NODE" : "REL";
        for (auto columnID = 0u; columnID < sharedState->columns.size(); columnID++) {
            appendStorageInfoForColumn(
                tableType, outputChunk, localState, sharedState->columns[columnID]);
        }
        localState->dataChunkCollection->append(outputChunk);
        outputChunk.resetAuxiliaryBuffer();
        outputChunk.state->selVector->selectedSize = 0;
    }
}

function_set StorageInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("storage_info", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void StorageInfoFunction::appendStorageInfoForColumn(std::string tableType, DataChunk& outputChunk,
    StorageInfoLocalState* localState, const Column* column) {
    auto numNodeGroups = column->getNumNodeGroups(&transaction::DUMMY_READ_TRANSACTION);
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        if (outputChunk.state->selVector->selectedSize == DEFAULT_VECTOR_CAPACITY) {
            localState->dataChunkCollection->append(outputChunk);
            outputChunk.resetAuxiliaryBuffer();
            outputChunk.state->selVector->selectedSize = 0;
        }
        appendColumnChunkStorageInfo(nodeGroupIdx, tableType, column, outputChunk);
    }
}

void StorageInfoFunction::appendColumnChunkStorageInfo(node_group_idx_t nodeGroupIdx,
    const std::string& tableType, const Column* column, DataChunk& outputChunk) {
    auto vectorPos = outputChunk.state->selVector->selectedSize;
    auto metadata = column->getMetadata(nodeGroupIdx, transaction::TransactionType::READ_ONLY);
    auto columnType = column->getDataType().toString();
    outputChunk.getValueVector(0)->setValue<uint64_t>(vectorPos, nodeGroupIdx);
    outputChunk.getValueVector(1)->setValue(vectorPos, column->getName());
    outputChunk.getValueVector(2)->setValue(vectorPos, columnType);
    outputChunk.getValueVector(3)->setValue(vectorPos, tableType);
    outputChunk.getValueVector(4)->setValue<uint64_t>(vectorPos, metadata.pageIdx);
    outputChunk.getValueVector(5)->setValue<uint64_t>(vectorPos, metadata.numPages);
    outputChunk.getValueVector(6)->setValue<uint64_t>(vectorPos, metadata.numValues);
    outputChunk.getValueVector(7)->setValue(vectorPos, metadata.compMeta.toString());
    outputChunk.state->selVector->selectedSize++;
}

} // namespace function
} // namespace kuzu
