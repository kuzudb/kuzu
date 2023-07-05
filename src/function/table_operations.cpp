#include "function/table_operations.h"

#include "catalog/catalog.h"

namespace kuzu {
namespace function {

void TableInfoOperation::tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors) {
    auto tableSchema = reinterpret_cast<function::TableInfoBindData*>(bindData)->tableSchema;
    auto numPropertiesToOutput = morsel.second - morsel.first;
    auto outVectorPos = 0;
    for (auto i = 0u; i < numPropertiesToOutput; i++) {
        auto property = tableSchema->properties[morsel.first + i];
        if (!tableSchema->isNodeTable && property.name == common::InternalKeyword::ID) {
            continue;
        }
        outputVectors[0]->setValue(outVectorPos, (int64_t)property.propertyID);
        outputVectors[1]->setValue(outVectorPos, property.name);
        outputVectors[2]->setValue(
            outVectorPos, common::LogicalTypeUtils::dataTypeToString(property.dataType));
        if (tableSchema->isNodeTable) {
            auto primaryKeyID =
                reinterpret_cast<catalog::NodeTableSchema*>(tableSchema)->primaryKeyPropertyID;
            outputVectors[3]->setValue(outVectorPos, primaryKeyID == property.propertyID);
        }
        outVectorPos++;
    }
    for (auto& outputVector : outputVectors) {
        outputVector->state->selVector->selectedSize = outVectorPos;
    }
}

std::unique_ptr<TableInfoBindData> TableInfoOperation::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<common::LogicalType> returnTypes;
    auto tableName = input.inputs[0].getValue<std::string>();
    auto tableID = catalog->getTableID(tableName);
    auto schema = catalog->getTableSchema(tableID);
    returnColumnNames.emplace_back("property id");
    returnTypes.emplace_back(common::LogicalTypeID::INT64);
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(common::LogicalTypeID::STRING);
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(common::LogicalTypeID::STRING);
    if (schema->isNodeTable) {
        returnColumnNames.emplace_back("primary key");
        returnTypes.emplace_back(common::LogicalTypeID::BOOL);
    }
    return std::make_unique<TableInfoBindData>(
        schema, std::move(returnTypes), std::move(returnColumnNames), schema->getNumProperties());
}

void DBVersionOperation::tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors) {
    auto outputVector = outputVectors[0];
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVectors[0]->setValue(pos, std::string(common::KUZU_VERSION));
    outputVectors[0]->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> DBVersionOperation::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    std::vector<std::string> returnColumnNames;
    std::vector<common::LogicalType> returnTypes;
    returnColumnNames.emplace_back("KUZU_Version");
    returnTypes.emplace_back(common::LogicalTypeID::STRING);
    return std::make_unique<TableFuncBindData>(
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

void CurrentSettingOperation::tableFunc(std::pair<common::offset_t, common::offset_t> morsel,
    function::TableFuncBindData* bindData, std::vector<common::ValueVector*> outputVectors) {
    auto currentSettingBindData = reinterpret_cast<CurrentSettingBindData*>(bindData);
    auto outputVector = outputVectors[0];
    auto pos = outputVector->state->selVector->selectedPositions[0];
    outputVectors[0]->setValue(pos, currentSettingBindData->result);
    outputVectors[0]->setNull(pos, false);
    outputVector->state->selVector->selectedSize = 1;
}

std::unique_ptr<TableFuncBindData> CurrentSettingOperation::bindFunc(main::ClientContext* context,
    kuzu::function::TableFuncBindInput input, catalog::CatalogContent* catalog) {
    auto optionName = input.inputs[0].getValue<std::string>();
    std::vector<std::string> returnColumnNames;
    std::vector<common::LogicalType> returnTypes;
    returnColumnNames.emplace_back(optionName);
    returnTypes.emplace_back(common::LogicalTypeID::STRING);
    return std::make_unique<CurrentSettingBindData>(context->getCurrentSetting(optionName),
        std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
}

} // namespace function
} // namespace kuzu
