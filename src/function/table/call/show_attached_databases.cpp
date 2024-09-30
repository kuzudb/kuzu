#include "function/table/call_functions.h"
#include "main/database_manager.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct ShowAttachedDatabasesBindData : public CallTableFuncBindData {
    std::vector<main::AttachedDatabase*> attachedDatabases;

    ShowAttachedDatabasesBindData(std::vector<main::AttachedDatabase*> attachedDatabases,
        std::vector<LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          attachedDatabases{std::move(attachedDatabases)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowAttachedDatabasesBindData>(attachedDatabases,
            LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& dataChunk = output.dataChunk;
    auto sharedState = input.sharedState->ptrCast<CallFuncSharedState>();
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto& attachedDatabases =
        input.bindData->constPtrCast<ShowAttachedDatabasesBindData>()->attachedDatabases;
    auto numDatabasesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numDatabasesToOutput; i++) {
        auto attachedDatabase = attachedDatabases[morsel.startOffset + i];
        dataChunk.getValueVectorMutable(0).setValue(i, attachedDatabase->getDBName());
        dataChunk.getValueVectorMutable(1).setValue(i, attachedDatabase->getDBType());
    }
    return numDatabasesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    ScanTableFuncBindInput*) {
    std::vector<std::string> columnNames;
    std::vector<LogicalType> columnTypes;
    columnNames.emplace_back("name");
    columnTypes.emplace_back(LogicalType::STRING());
    columnNames.emplace_back("database type");
    columnTypes.emplace_back(LogicalType::STRING());
    auto attachedDatabases = context->getDatabaseManager()->getAttachedDatabases();
    return std::make_unique<ShowAttachedDatabasesBindData>(attachedDatabases,
        std::move(columnTypes), std::move(columnNames), attachedDatabases.size());
}

function_set ShowAttachedDatabasesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        initSharedState, initEmptyLocalState, std::vector<LogicalTypeID>{}));
    return functionSet;
}

} // namespace function
} // namespace kuzu
