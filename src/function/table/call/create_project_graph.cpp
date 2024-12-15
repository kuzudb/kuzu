#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/types/value/nested.h"
#include "function/table/simple_table_functions.h"
#include "graph/graph_entry.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct CreateProjectGraphBindData : SimpleTableFuncBindData {
    std::string graphName;
    std::vector<TableCatalogEntry*> nodeEntries;
    std::vector<TableCatalogEntry*> relEntries;

    CreateProjectGraphBindData(std::string graphName, std::vector<TableCatalogEntry*> nodeEntries,
        std::vector<TableCatalogEntry*> relEntries)
        : SimpleTableFuncBindData{0}, graphName{graphName}, nodeEntries{nodeEntries},
          relEntries{relEntries} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateProjectGraphBindData>(graphName, nodeEntries, relEntries);
    }
};

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto bindData = ku_dynamic_cast<CreateProjectGraphBindData*>(input.bindData);
    auto& graphEntrySet = input.context->clientContext->getGraphEntrySetUnsafe();
    if (graphEntrySet.hasGraph(bindData->graphName)) {
        throw RuntimeException(
            stringFormat("Project graph {} already exists.", bindData->graphName));
    }
    auto entry = graph::GraphEntry(bindData->nodeEntries, bindData->relEntries);
    graphEntrySet.addGraph(bindData->graphName, std::move(entry));
    return 0;
}

static std::vector<std::string> getAsStringVector(const Value& value) {
    std::vector<std::string> result;
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
        result.push_back(NestedVal::getChildVal(&value, i)->getValue<std::string>());
    }
    return result;
}

static std::vector<catalog::TableCatalogEntry*> getTableEntries(
    const std::vector<std::string>& tableNames, CatalogEntryType expectedType,
    main::ClientContext& context) {
    std::vector<catalog::TableCatalogEntry*> entries;
    for (auto& tableName : tableNames) {
        auto entry = context.getCatalog()->getTableCatalogEntry(context.getTx(), tableName);
        if (entry->getType() != expectedType) {
            throw BinderException(stringFormat("Expect catalog entry type {} but got {}.",
                CatalogEntryTypeUtils::toString(expectedType),
                CatalogEntryTypeUtils::toString(entry->getType())));
        }
        entries.push_back(entry);
    }
    return entries;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto nodeTableNames = getAsStringVector(input->getValue(1));
    auto relTableNames = getAsStringVector(input->getValue(2));
    auto nodeEntries =
        getTableEntries(nodeTableNames, CatalogEntryType::NODE_TABLE_ENTRY, *context);
    auto relEntries = getTableEntries(relTableNames, CatalogEntryType::REL_TABLE_ENTRY, *context);
    return std::make_unique<CreateProjectGraphBindData>(graphName, nodeEntries, relEntries);
}

function_set CreateProjectGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto func =
        std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING,
                                                  LogicalTypeID::LIST, LogicalTypeID::LIST});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = initSharedState;
    func->initLocalStateFunc = initEmptyLocalState;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
