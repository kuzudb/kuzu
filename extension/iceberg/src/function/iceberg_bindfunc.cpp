#include "common/exception/runtime.h"
#include "connector/iceberg_connector.h"
#include "function/iceberg_functions.h"

namespace kuzu {
namespace iceberg_extension {

using namespace function;
using namespace common;

static std::string generateQueryOptions(const TableFuncBindInput* input,
    const std::string& functionName) {
    std::string query_options = "";
    auto appendOptions = [&](const auto& options) {
        for (auto& [name, value] : options) {
            auto lowerCaseName = StringUtils::getLower(name);
            auto valueStr = value.toString();
            if (lowerCaseName == "allow_moved_paths") {
                // check data type of allow_moved_paths
                if (value.getDataType().getLogicalTypeID() != LogicalTypeID::BOOL) {
                    throw RuntimeException{
                        common::stringFormat("Invalid allow_moved_paths value for {}", valueStr)};
                }
                query_options += common::stringFormat(", {} = {}", lowerCaseName, valueStr);
            } else {
                query_options += common::stringFormat(", {} = '{}'", lowerCaseName, valueStr);
            }
        }
    };
    if (functionName == "ICEBERG_SCAN") {
        auto scanInput = input->extraInput->constPtrCast<ExtraScanTableFuncBindInput>();
        appendOptions(scanInput->fileScanInfo.options);
    } else {
        appendOptions(input->optionalParams);
    }

    return query_options;
}

std::unique_ptr<TableFuncBindData> bindFuncHelper(main::ClientContext* context,
    const TableFuncBindInput* input, const std::string& functionName) {
    auto connector = std::make_shared<IcebergConnector>();
    connector->connect("" /* inMemDB */, "" /* defaultCatalogName */, "" /* defaultSchemaName */,
        context);

    std::string query_options = generateQueryOptions(input, functionName);
    std::string query = stringFormat("SELECT * FROM {}('{}'{})", functionName,
        input->getLiteralVal<std::string>(0), query_options);
    auto result = connector->executeQuery(query + " LIMIT 1");

    std::vector<LogicalType> returnTypes;
    std::vector<std::string> returnColumnNames;
    // only ICEBERG_SCAN uses scanInput
    if (functionName == "ICEBERG_SCAN") {
        auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
        returnColumnNames = scanInput->expectedColumnNames;
        if (scanInput->expectedColumnNames.empty()) {
            for (auto name : result->names) {
                returnColumnNames.push_back(name);
            }
        }
    }
    for (auto type : result->types) {
        returnTypes.push_back(
            duckdb_extension::DuckDBTypeConverter::convertDuckDBType(type.ToString()));
    }
    if (functionName != "ICEBERG_SCAN") {
        for (auto name : result->names) {
            returnColumnNames.push_back(name);
        }
    }
    KU_ASSERT(returnTypes.size() == returnColumnNames.size());
    returnColumnNames =
        TableFunction::extractYieldVariables(returnColumnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    return std::make_unique<delta_extension::DeltaScanBindData>(std::move(query), connector,
        duckdb_extension::DuckDBResultConverter{returnTypes}, columns, FileScanInfo{}, context);
}
} // namespace iceberg_extension
} // namespace kuzu
