#include "function/table_functions/call_functions.h"

#include "catalog/node_table_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/exception/not_implemented.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::main;

function_set CurrentSettingFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("current_setting", tableFunc, bindFunc,
        initSharedState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
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

std::unique_ptr<SharedTableFuncState> CallFunction::initSharedState(TableFunctionInitInput& input) {
    auto callTableFuncBindData = reinterpret_cast<CallTableFuncBindData*>(input.bindData);
    return std::make_unique<CallFuncSharedState>(callTableFuncBindData->maxOffset);
}

void CurrentSettingFunction::tableFunc(TableFunctionInput& data, std::vector<ValueVector*> output) {
    auto sharedState = reinterpret_cast<CallFuncSharedState*>(data.sharedState);
    auto outputVector = output[0];
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        outputVector->state->selVector->selectedSize = 0;
        return;
    }
    auto currentSettingBindData = reinterpret_cast<CurrentSettingBindData*>(data.bindData);
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVector->setValue(pos, currentSettingBindData->result);
    outputVector->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> CurrentSettingFunction::bindFunc(
    ClientContext* context, TableFuncBindInput input, CatalogContent* /*catalog*/) {
    auto optionName = input.inputs[0]->getValue<std::string>();
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back(optionName);
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<CurrentSettingBindData>(context->getCurrentSetting(optionName),
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

function_set DBVersionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(
        "db_version", tableFunc, bindFunc, initSharedState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

void DBVersionFunction::tableFunc(
    TableFunctionInput& input, std::vector<ValueVector*> outputVectors) {
    auto sharedState = reinterpret_cast<CallFuncSharedState*>(input.sharedState);
    auto outputVector = outputVectors[0];
    if (!sharedState->getMorsel().hasMoreToOutput()) {
        outputVector->state->selVector->selectedSize = 0;
        return;
    }
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVector->setValue(pos, std::string(KUZU_VERSION));
    outputVector->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> DBVersionFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput /*input*/, CatalogContent* /*catalog*/) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<CallTableFuncBindData>(
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

function_set ShowTablesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(
        "show_tables", tableFunc, bindFunc, initSharedState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

void ShowTablesFunction::tableFunc(
    TableFunctionInput& input, std::vector<ValueVector*> outputVectors) {
    auto sharedState = reinterpret_cast<CallFuncSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputVectors[0]->state->selVector->selectedSize = 0;
        return;
    }
    auto tables = reinterpret_cast<function::ShowTablesBindData*>(input.bindData)->tables;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto tableSchema = tables[morsel.startOffset + i];
        outputVectors[0]->setValue(i, tableSchema->tableName);
        std::string typeString = TableTypeUtils::toString(tableSchema->tableType);
        outputVectors[1]->setValue(i, typeString);
        outputVectors[2]->setValue(i, tableSchema->comment);
    }
    outputVectors[0]->state->selVector->selectedSize = numTablesToOutput;
}

std::unique_ptr<TableFuncBindData> ShowTablesFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput /*input*/, CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    returnColumnNames.emplace_back("comment");
    returnTypes.emplace_back(LogicalTypeID::STRING);

    return std::make_unique<ShowTablesBindData>(catalog->getTableSchemas(), std::move(returnTypes),
        std::move(returnColumnNames), catalog->getTableCount());
}

function_set TableInfoFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("table_info", tableFunc, bindFunc,
        initSharedState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void TableInfoFunction::tableFunc(
    TableFunctionInput& input, std::vector<ValueVector*> outputVectors) {
    auto morsel = reinterpret_cast<CallFuncSharedState*>(input.sharedState)->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputVectors[0]->state->selVector->selectedSize = 0;
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
        outputVectors[0]->setValue(outVectorPos, (int64_t)property->getPropertyID());
        outputVectors[1]->setValue(outVectorPos, property->getName());
        outputVectors[2]->setValue(outVectorPos, property->getDataType()->toString());
        if (tableSchema->tableType == TableType::NODE) {
            auto primaryKeyID =
                reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKeyPropertyID();
            outputVectors[3]->setValue(outVectorPos, primaryKeyID == property->getPropertyID());
        }
        outVectorPos++;
    }
    for (auto& outputVector : outputVectors) {
        outputVector->state->selVector->selectedSize = outVectorPos;
    }
}

std::unique_ptr<TableFuncBindData> TableInfoFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput input, CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    auto tableName = input.inputs[0]->getValue<std::string>();
    auto tableID = catalog->getTableID(tableName);
    auto schema = catalog->getTableSchema(tableID);
    returnColumnNames.emplace_back("property id");
    returnTypes.emplace_back(LogicalTypeID::INT64);
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    if (schema->tableType == TableType::NODE) {
        returnColumnNames.emplace_back("primary key");
        returnTypes.emplace_back(LogicalTypeID::BOOL);
    }
    return std::make_unique<TableInfoBindData>(
        schema, std::move(returnTypes), std::move(returnColumnNames), schema->getNumProperties());
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>("db_version", tableFunc, bindFunc,
        initSharedState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
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

void ShowConnectionFunction::tableFunc(
    TableFunctionInput& input, std::vector<ValueVector*> outputVectors) {
    auto morsel = reinterpret_cast<CallFuncSharedState*>(input.sharedState)->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        outputVectors[0]->state->selVector->selectedSize = 0;
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
        outputRelTableConnection(
            outputVectors[0], outputVectors[1], vectorPos, catalog, tableSchema->tableID);
        vectorPos++;
    } break;
    case TableType::REL_GROUP: {
        auto schema = reinterpret_cast<RelTableGroupSchema*>(tableSchema);
        auto relTableIDs = schema->getRelTableIDs();
        for (; vectorPos < numRelationsToOutput; vectorPos++) {
            outputRelTableConnection(outputVectors[0], outputVectors[1], vectorPos, catalog,
                relTableIDs[morsel.startOffset + vectorPos]);
        }
    } break;
    default:
        // LCOV_EXCL_START
        throw NotImplementedException{"ShowConnectionFunction::tableFunc"};
        // LCOV_EXCL_STOP
    }
    for (auto& outputVector : outputVectors) {
        outputVector->state->selVector->selectedSize = vectorPos;
    }
}

std::unique_ptr<TableFuncBindData> ShowConnectionFunction::bindFunc(
    ClientContext* /*context*/, TableFuncBindInput input, CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    auto tableName = input.inputs[0]->getValue<std::string>();
    auto tableID = catalog->getTableID(tableName);
    auto schema = catalog->getTableSchema(tableID);
    auto tableType = schema->getTableType();
    if (tableType != TableType::REL && tableType != TableType::REL_GROUP) {
        throw BinderException{"Show connection can only be called on a rel table!"};
    }
    returnColumnNames.emplace_back("source table name");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    returnColumnNames.emplace_back("destination table name");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<ShowConnectionBindData>(catalog, schema, std::move(returnTypes),
        std::move(returnColumnNames),
        schema->tableType == TableType::REL ?
            1 :
            reinterpret_cast<RelTableGroupSchema*>(schema)->getRelTableIDs().size());
}

} // namespace function
} // namespace kuzu
