#include "function/table_functions/table_info.h"

#include "catalog/catalog_content.h"
#include "catalog/node_table_schema.h"
#include "catalog/table_schema.h"
#include "common/vector/value_vector.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
struct TableInfoBindData : public TableFuncBindData {
    catalog::TableSchema* tableSchema;

    TableInfoBindData(catalog::TableSchema* tableSchema,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : tableSchema{tableSchema}, TableFuncBindData{std::move(returnTypes),
                                        std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<TableInfoBindData>(
            tableSchema, returnTypes, returnColumnNames, maxOffset);
    }
};

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

std::unique_ptr<TableFuncBindData> TableInfoFunction::bindFunc(main::ClientContext* context,
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
} // namespace function
} // namespace kuzu
