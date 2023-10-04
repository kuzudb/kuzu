#include "function/built_in_table_functions.h"

#include "catalog/catalog_content.h"
#include "catalog/node_table_schema.h"
#include "common/exception/binder.h"
#include "common/expression_type.h"
#include "common/string_utils.h"
#include "common/vector/value_vector.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
struct CurrentSettingBindData : public TableFuncBindData {
    std::string result;

    CurrentSettingBindData(std::string result, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : result{std::move(result)}, TableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<CurrentSettingBindData>(
            result, returnTypes, returnColumnNames, maxOffset);
    }
};

struct CurrentSettingFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("current_setting", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<offset_t, offset_t> morsel, TableFuncBindData* bindData,
        std::vector<ValueVector*> outputVectors) {
        auto currentSettingBindData = reinterpret_cast<CurrentSettingBindData*>(bindData);
        auto outputVector = outputVectors[0];
        auto pos = outputVector->state->selVector->selectedPositions[0];
        outputVectors[0]->setValue(pos, currentSettingBindData->result);
        outputVectors[0]->setNull(pos, false);
        outputVector->state->selVector->selectedSize = 1;
    }

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog) {
        auto optionName = input.inputs[0].getValue<std::string>();
        std::vector<std::string> returnColumnNames;
        std::vector<LogicalType> returnTypes;
        returnColumnNames.emplace_back(optionName);
        returnTypes.emplace_back(LogicalTypeID::STRING);
        return std::make_unique<CurrentSettingBindData>(context->getCurrentSetting(optionName),
            std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
    }
};

struct DBVersionFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("db_version", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<offset_t, offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
        auto outputVector = outputVectors[0];
        auto pos = outputVector->state->selVector->selectedPositions[0];
        outputVectors[0]->setValue(pos, std::string(KUZU_VERSION));
        outputVectors[0]->setNull(pos, false);
        outputVector->state->selVector->selectedSize = 1;
    }

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog) {
        std::vector<std::string> returnColumnNames;
        std::vector<LogicalType> returnTypes;
        returnColumnNames.emplace_back("version");
        returnTypes.emplace_back(LogicalTypeID::STRING);
        return std::make_unique<TableFuncBindData>(
            std::move(returnTypes), std::move(returnColumnNames), 1 /* one row result */);
    }
};

struct ShowTablesBindData : public TableFuncBindData {
    std::vector<catalog::TableSchema*> tables;

    ShowTablesBindData(std::vector<catalog::TableSchema*> tables,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : tables{std::move(tables)}, TableFuncBindData{std::move(returnTypes),
                                         std::move(returnColumnNames), maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ShowTablesBindData>(
            tables, returnTypes, returnColumnNames, maxOffset);
    }
};

struct ShowTablesFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("show_tables", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<offset_t, offset_t> morsel,
        function::TableFuncBindData* bindData, std::vector<ValueVector*> outputVectors) {
        auto tables = reinterpret_cast<function::ShowTablesBindData*>(bindData)->tables;
        auto numTablesToOutput = morsel.second - morsel.first;
        for (auto i = 0u; i < numTablesToOutput; i++) {
            auto tableSchema = tables[morsel.first + i];
            outputVectors[0]->setValue(i, tableSchema->tableName);
            std::string typeString = TableTypeUtils::toString(tableSchema->tableType);
            outputVectors[1]->setValue(i, typeString);
            outputVectors[2]->setValue(i, tableSchema->comment);
        }
        outputVectors[0]->state->selVector->selectedSize = numTablesToOutput;
    }

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog) {
        std::vector<std::string> returnColumnNames;
        std::vector<LogicalType> returnTypes;
        returnColumnNames.emplace_back("name");
        returnTypes.emplace_back(LogicalTypeID::STRING);
        returnColumnNames.emplace_back("type");
        returnTypes.emplace_back(LogicalTypeID::STRING);
        returnColumnNames.emplace_back("comment");
        returnTypes.emplace_back(LogicalTypeID::STRING);

        return std::make_unique<ShowTablesBindData>(catalog->getTableSchemas(),
            std::move(returnTypes), std::move(returnColumnNames), catalog->getTableCount());
    }
};

struct TableInfoBindData : public TableFuncBindData {
    catalog::TableSchema* tableSchema;

    TableInfoBindData(catalog::TableSchema* tableSchema, std::vector<LogicalType> returnTypes,
        std::vector<std::string> returnColumnNames, offset_t maxOffset)
        : tableSchema{tableSchema}, TableFuncBindData{std::move(returnTypes),
                                        std::move(returnColumnNames), maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<TableInfoBindData>(
            tableSchema, returnTypes, returnColumnNames, maxOffset);
    }
};

struct TableInfoFunction {
    inline static std::unique_ptr<TableFunctionDefinition> getDefinitions() {
        return std::make_unique<TableFunctionDefinition>("table_info", tableFunc, bindFunc);
    }

    static void tableFunc(std::pair<offset_t, offset_t> morsel, TableFuncBindData* bindData,
        std::vector<ValueVector*> outputVectors) {
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
                auto primaryKeyID = reinterpret_cast<catalog::NodeTableSchema*>(tableSchema)
                                        ->getPrimaryKeyPropertyID();
                outputVectors[3]->setValue(outVectorPos, primaryKeyID == property->getPropertyID());
            }
            outVectorPos++;
        }
        for (auto& outputVector : outputVectors) {
            outputVector->state->selVector->selectedSize = outVectorPos;
        }
    }

    static std::unique_ptr<TableFuncBindData> bindFunc(
        main::ClientContext* context, TableFuncBindInput input, catalog::CatalogContent* catalog) {
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
        return std::make_unique<TableInfoBindData>(schema, std::move(returnTypes),
            std::move(returnColumnNames), schema->getNumProperties());
    }
};

void BuiltInTableFunctions::registerTableFunctions() {
    tableFunctions.insert({CURRENT_SETTING_FUNC_NAME, CurrentSettingFunction::getDefinitions()});
    tableFunctions.insert({DB_VERSION_FUNC_NAME, DBVersionFunction::getDefinitions()});
    tableFunctions.insert({SHOW_TABLES_FUNC_NAME, ShowTablesFunction::getDefinitions()});
    tableFunctions.insert({TABLE_INFO_FUNC_NAME, TableInfoFunction::getDefinitions()});
}

TableFunctionDefinition* BuiltInTableFunctions::mathTableFunction(const std::string& name) {
    auto upperName = name;
    StringUtils::toUpper(upperName);
    if (!tableFunctions.contains(upperName)) {
        throw BinderException{"Cannot match a built-in function for given function " + name + "."};
    }
    return tableFunctions.at(upperName).get();
}

} // namespace function
} // namespace kuzu
