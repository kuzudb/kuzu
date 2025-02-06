#include "binder/binder.h"
#include "extension/extension.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

static constexpr std::pair<std::string_view, std::string_view> extensions[] = {
    {"HTTPFS", "Adds support for reading and writing files over a HTTP(S)/S3 filesystem"},
    {"DELTA", "Adds support for reading from delta tables"},
    {"DUCKDB", "Adds support for reading from duckdb tables"},
    {"FTS", "Adds support for full-text search indexes"},
    {"ICEBERG", "Adds support for reading from iceberg tables"},
    {"JSON", "Adds support for JSON operations"},
    {"POSTGRES", "Adds support for reading from POSTGRES tables"},
    {"SQLITE", "Adds support for reading from SQLITE tables"}};
static constexpr auto officialExtensions = std::to_array(extensions);

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = input.sharedState->ptrCast<TableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto numTuplesToOutput = morsel.endOffset - morsel.startOffset;
    for (uint64_t i = 0; i < numTuplesToOutput; ++i) {
        auto& [name, description] = officialExtensions[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, name);
        dataChunk.getValueVectorMutable(1).setValue(i, description);
    }
    return numTuplesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* /*context*/,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("description");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<TableFuncBindData>(std::move(columns), officialExtensions.size());
}

function_set ShowOfficialExtensionsFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<common::LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = TableFunction::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
