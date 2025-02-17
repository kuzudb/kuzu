#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"
#include "main/database_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct ShowAttachedDatabasesBindData final : TableFuncBindData {
    std::vector<main::AttachedDatabase*> attachedDatabases;

    ShowAttachedDatabasesBindData(std::vector<main::AttachedDatabase*> attachedDatabases,
        binder::expression_vector columns, offset_t maxOffset)
        : TableFuncBindData{std::move(columns), maxOffset},
          attachedDatabases{std::move(attachedDatabases)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowAttachedDatabasesBindData>(attachedDatabases, columns,
            maxOffset);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    const auto sharedState = input.sharedState->ptrCast<TableFuncSharedState>();
    const auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto& attachedDatabases =
        input.bindData->constPtrCast<ShowAttachedDatabasesBindData>()->attachedDatabases;
    const auto numDatabasesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numDatabasesToOutput; i++) {
        const auto attachedDatabase = attachedDatabases[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, attachedDatabase->getDBName());
        dataChunk.getValueVectorMutable(1).setValue(i, attachedDatabase->getDBType());
    }
    return numDatabasesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("database type");
    columnTypes.emplace_back(LogicalType::STRING());
    auto attachedDatabases = context->getDatabaseManager()->getAttachedDatabases();
    columnNames = TableFunction::extractYieldVariables(columnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(columnNames, columnTypes);
    return std::make_unique<ShowAttachedDatabasesBindData>(attachedDatabases, columns,
        attachedDatabases.size());
}

function_set ShowAttachedDatabasesFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = TableFunction::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
