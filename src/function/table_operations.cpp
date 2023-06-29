#include "function/table_operations.h"

#include "catalog/catalog.h"

namespace kuzu {
namespace function {

std::unique_ptr<TableInfoBindData> TableInfoOperation::bindFunc(
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

} // namespace function
} // namespace kuzu
