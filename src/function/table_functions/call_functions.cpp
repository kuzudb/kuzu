#include "function/table_functions/call_functions.h"

#include "catalog/node_table_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::main;

std::unique_ptr<TableFuncLocalState> initLocalState(
    TableFunctionInitInput& /*input*/, TableFuncSharedState* /*state*/) {
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
    auto callTableFuncBindData = reinterpret_cast<CallTableFuncBindData*>(input.bindData);
    return std::make_unique<CallFuncSharedState>(callTableFuncBindData->maxOffset);
}

void CurrentSettingFunction::tableFunc(TableFunctionInput& data, DataChunk& output) {
    auto sharedState = reinterpret_cast<CallFuncSharedState*>(data.sharedState);
    auto outputVector = output.getValueVector(0);
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        output.state->selVector->selectedSize = 0;
        return;
    }
    auto currentSettingBindData = reinterpret_cast<CurrentSettingBindData*>(data.bindData);
    auto pos = output.state->selVector->selectedPositions[0];
    outputVector->setValue(pos, currentSettingBindData->result);
    outputVector->setNull(pos, false);
    output.state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> CurrentSettingFunction::bindFunc(
    ClientContext* context, TableFuncBindInput* input, CatalogContent* /*catalog*/) {
    auto optionName = input->inputs[0]->getValue<std::string>();
    std::vector<std::string> returnColumnNames;
    std::vector<std::unique_ptr<LogicalType>> returnTypes;
    returnColumnNames.emplace_back(optionName);
    returnTypes.push_back(LogicalType::STRING());
    return std::make_unique<CurrentSettingBindData>(context->getCurrentSetting(optionName),
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

function_set DBVersionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("db_version", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

void DBVersionFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto sharedState = reinterpret_cast<CallFuncSharedState*>(input.sharedState);
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

std::unique_ptr<TableFuncBindData> DBVersionFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput* /*input*/, CatalogContent* /*catalog*/) {
    std::vector<std::string> returnColumnNames;
    std::vector<std::unique_ptr<LogicalType>> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(LogicalType::STRING());
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
    auto sharedState = reinterpret_cast<CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.getValueVector(0)->state->selVector->selectedSize = 0;
        return;
    }
    auto tables = reinterpret_cast<function::ShowTablesBindData*>(input.bindData)->tables;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto tableSchema = tables[morsel.startOffset + i];
        outputChunk.getValueVector(0)->setValue(i, tableSchema->tableName);
        std::string typeString = TableTypeUtils::toString(tableSchema->tableType);
        outputChunk.getValueVector(1)->setValue(i, typeString);
        outputChunk.getValueVector(2)->setValue(i, tableSchema->comment);
    }
    outputChunk.state->selVector->selectedSize = numTablesToOutput;
}

std::unique_ptr<TableFuncBindData> ShowTablesFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput* /*input*/, CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<std::unique_ptr<LogicalType>> returnTypes;
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(LogicalType::STRING());
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(LogicalType::STRING());
    returnColumnNames.emplace_back("comment");
    returnTypes.emplace_back(LogicalType::STRING());
    return std::make_unique<ShowTablesBindData>(catalog->getTableSchemas(), std::move(returnTypes),
        std::move(returnColumnNames), catalog->getTableCount());
}

function_set TableInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("table_info", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void TableInfoFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto morsel = reinterpret_cast<CallFuncSharedState*>(input.sharedState)->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.getValueVector(0)->state->selVector->selectedSize = 0;
        return;
    }
    auto tableSchema = reinterpret_cast<function::TableInfoBindData*>(input.bindData)->tableSchema;
    auto numPropertiesToOutput = morsel.endOffset - morsel.startOffset;
    auto outVectorPos = 0;
    for (auto i = 0u; i < numPropertiesToOutput; i++) {
        auto property = tableSchema->properties[morsel.startOffset + i].get();
        if (tableSchema->getTableType() == TableType::REL &&
            property->getName() == InternalKeyword::ID) {
            continue;
        }
        outputChunk.getValueVector(0)->setValue(outVectorPos, (int64_t)property->getPropertyID());
        outputChunk.getValueVector(1)->setValue(outVectorPos, property->getName());
        outputChunk.getValueVector(2)->setValue(outVectorPos, property->getDataType()->toString());

        if (tableSchema->tableType == TableType::NODE) {
            auto primaryKeyID =
                reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKeyPropertyID();
            outputChunk.getValueVector(3)->setValue(
                outVectorPos, primaryKeyID == property->getPropertyID());
        }
        outVectorPos++;
    }
    outputChunk.state->selVector->selectedSize = outVectorPos;
}

std::unique_ptr<TableFuncBindData> TableInfoFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput* input, CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<std::unique_ptr<LogicalType>> returnTypes;
    auto tableName = input->inputs[0]->getValue<std::string>();
    auto tableID = catalog->getTableID(tableName);
    auto schema = catalog->getTableSchema(tableID);
    returnColumnNames.emplace_back("property id");
    returnTypes.push_back(LogicalType::INT64());
    returnColumnNames.emplace_back("name");
    returnTypes.push_back(LogicalType::STRING());
    returnColumnNames.emplace_back("type");
    returnTypes.push_back(LogicalType::STRING());
    if (schema->tableType == TableType::NODE) {
        returnColumnNames.emplace_back("primary key");
        returnTypes.push_back(LogicalType::BOOL());
    }
    return std::make_unique<TableInfoBindData>(
        schema, std::move(returnTypes), std::move(returnColumnNames), schema->getNumProperties());
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("db_version", tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void ShowConnectionFunction::outputRelTableConnection(ValueVector* srcTableNameVector,
    ValueVector* dstTableNameVector, uint64_t outputPos, CatalogContent* catalog,
    table_id_t tableID) {
    auto tableSchema = catalog->getTableSchema(tableID);
    KU_ASSERT(tableSchema->tableType == TableType::REL);
    auto srcTableID = reinterpret_cast<RelTableSchema*>(tableSchema)->getSrcTableID();
    auto dstTableID = reinterpret_cast<RelTableSchema*>(tableSchema)->getDstTableID();
    srcTableNameVector->setValue(outputPos, catalog->getTableName(srcTableID));
    dstTableNameVector->setValue(outputPos, catalog->getTableName(dstTableID));
}

void ShowConnectionFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto morsel = reinterpret_cast<CallFuncSharedState*>(input.sharedState)->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputChunk.state->selVector->selectedSize = 0;
        return;
    }
    auto showConnectionBindData =
        reinterpret_cast<function::ShowConnectionBindData*>(input.bindData);
    auto tableSchema = showConnectionBindData->tableSchema;
    auto numRelationsToOutput = morsel.endOffset - morsel.startOffset;
    auto catalog = showConnectionBindData->catalog;
    auto vectorPos = 0;
    switch (tableSchema->getTableType()) {
    case TableType::REL: {
        outputRelTableConnection(outputChunk.getValueVector(0).get(),
            outputChunk.getValueVector(1).get(), vectorPos, catalog, tableSchema->tableID);
        vectorPos++;
    } break;
    case TableType::REL_GROUP: {
        auto schema = reinterpret_cast<RelTableGroupSchema*>(tableSchema);
        auto relTableIDs = schema->getRelTableIDs();
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

std::unique_ptr<TableFuncBindData> ShowConnectionFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput* input, CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<std::unique_ptr<LogicalType>> returnTypes;
    auto tableName = input->inputs[0]->getValue<std::string>();
    auto tableID = catalog->getTableID(tableName);
    auto schema = catalog->getTableSchema(tableID);
    auto tableType = schema->getTableType();
    if (tableType != TableType::REL && tableType != TableType::REL_GROUP) {
        throw BinderException{"Show connection can only be called on a rel table!"};
    }
    returnColumnNames.emplace_back("source table name");
    returnTypes.emplace_back(LogicalType::STRING());
    returnColumnNames.emplace_back("destination table name");
    returnTypes.emplace_back(LogicalType::STRING());
    return std::make_unique<ShowConnectionBindData>(catalog, schema, std::move(returnTypes),
        std::move(returnColumnNames),
        schema->tableType == TableType::REL ?
            1 :
            reinterpret_cast<RelTableGroupSchema*>(schema)->getRelTableIDs().size());
}

} // namespace function
} // namespace kuzu
