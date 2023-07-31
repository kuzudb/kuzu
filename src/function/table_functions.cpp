#include "function/table_functions.h"

#include "catalog/catalog.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace function {

void TableInfoFunction::tableFunc(std::pair<offset_t, offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
    auto tableSchema = reinterpret_cast<function::TableInfoBindData*>(bindData)->tableSchema;
    auto numPropertiesToOutput = morsel.second - morsel.first;
    auto outVectorPos = 0;
    for (auto i = 0u; i < numPropertiesToOutput; i++) {
        auto property = tableSchema->properties[morsel.first + i].get();
        if (tableSchema->getTableType() == TableType::REL &&
            property->getName() == InternalKeyword::ID) {
            continue;
        }
        outputVectors[0]->setValue(outVectorPos, (int64_t)property->getPropertyID());
        outputVectors[1]->setValue(outVectorPos, property->getName());
        outputVectors[2]->setValue(
            outVectorPos, LogicalTypeUtils::dataTypeToString(*property->getDataType()));
        if (tableSchema->tableType == TableType::NODE) {
            auto primaryKeyID =
                reinterpret_cast<catalog::NodeTableSchema*>(tableSchema)->getPrimaryKeyPropertyID();
            outputVectors[3]->setValue(outVectorPos, primaryKeyID == property->getPropertyID());
        }
        outVectorPos++;
    }
    for (auto& outputVector : outputVectors) {
        outputVector->state->selVector->selectedSize = outVectorPos;
    }
}

std::unique_ptr<TableInfoBindData> TableInfoFunction::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    auto tableName = input.inputs[0].getValue<std::string>();
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

void DBVersionFunction::tableFunc(std::pair<offset_t, offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
    auto outputVector = outputVectors[0];
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVectors[0]->setValue(pos, std::string(KUZU_VERSION));
    outputVectors[0]->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> DBVersionFunction::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("version");
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<TableFuncBindData>(
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

void CurrentSettingFunction::tableFunc(std::pair<offset_t, offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
    auto currentSettingBindData = reinterpret_cast<CurrentSettingBindData*>(bindData);
    auto outputVector = outputVectors[0];
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVectors[0]->setValue(pos, currentSettingBindData->result);
    outputVectors[0]->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> CurrentSettingFunction::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    auto optionName = input.inputs[0].getValue<std::string>();
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back(optionName);
    returnTypes.emplace_back(LogicalTypeID::STRING);
    return std::make_unique<CurrentSettingBindData>(context->getCurrentSetting(optionName),
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

} // namespace function
} // namespace kuzu
