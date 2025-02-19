#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/types/value/nested.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"
#include "graph/graph_entry.h"
#include "processor/execution_context.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

struct CreateProjectGraphBindData final : TableFuncBindData {
    std::string graphName;
    std::vector<TableCatalogEntry*> nodeEntries;
    std::vector<TableCatalogEntry*> relEntries;

    CreateProjectGraphBindData(std::string graphName, std::vector<TableCatalogEntry*> nodeEntries,
        std::vector<TableCatalogEntry*> relEntries)
        : TableFuncBindData{0}, graphName{std::move(graphName)},
          nodeEntries{std::move(nodeEntries)}, relEntries{std::move(relEntries)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateProjectGraphBindData>(graphName, nodeEntries, relEntries);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto bindData = ku_dynamic_cast<CreateProjectGraphBindData*>(input.bindData);
    auto& graphEntrySet = input.context->clientContext->getGraphEntrySetUnsafe();
    if (graphEntrySet.hasGraph(bindData->graphName)) {
        throw RuntimeException(
            stringFormat("Project graph {} already exists.", bindData->graphName));
    }
    auto entry = graph::GraphEntry();
    for (auto& nodeEntry : bindData->nodeEntries) {
        entry.nodeInfos.emplace_back(nodeEntry);
    }
    for (auto& relEntry : bindData->relEntries) {
        entry.relInfos.emplace_back(relEntry);
    }
    graphEntrySet.addGraph(bindData->graphName, entry);
    return 0;
}

static std::vector<std::string> getAsStringVector(const Value& value) {
    std::vector<std::string> result;
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
        result.push_back(NestedVal::getChildVal(&value, i)->getValue<std::string>());
    }
    return result;
}

static void validateEntryType(const TableCatalogEntry& entry, CatalogEntryType type) {
    if (entry.getType() != type) {
        throw BinderException(stringFormat("Expect catalog entry type {} but got {}.",
            CatalogEntryTypeUtils::toString(type),
            CatalogEntryTypeUtils::toString(entry.getType())));
    }
}

static void validateNodeProjected(table_id_t tableID, const table_id_set_t& projectedNodeIDSet,
    const std::string& relName, Catalog* catalog, transaction::Transaction* transaction) {
    if (projectedNodeIDSet.contains(tableID)) {
        return;
    }
    auto entryName = catalog->getTableCatalogEntry(transaction, tableID)->getName();
    throw BinderException(
        stringFormat("{} is connected to {} but not projected.", entryName, relName));
}

static void validateRelSrcDstNodeAreProjected(const TableCatalogEntry& entry,
    const table_id_set_t& projectedNodeIDSet, Catalog* catalog,
    transaction::Transaction* transaction) {
    auto& relEntry = entry.constCast<RelTableCatalogEntry>();
    validateNodeProjected(relEntry.getSrcTableID(), projectedNodeIDSet, entry.getName(), catalog,
        transaction);
    validateNodeProjected(relEntry.getDstTableID(), projectedNodeIDSet, entry.getName(), catalog,
        transaction);
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto graphName = input->getLiteralVal<std::string>(0);
    auto arg2 = input->getValue(1);
    auto arg3 = input->getValue(2);
    const auto nodeTableNames = getAsStringVector(arg2);
    const auto relTableNames = getAsStringVector(arg3);
    std::vector<TableCatalogEntry*> nodeEntries;
    common::table_id_set_t nodeTableIDSet;
    for (auto& name : nodeTableNames) {
        auto entry = catalog->getTableCatalogEntry(transaction, name, false /* useInternal */);
        validateEntryType(*entry, CatalogEntryType::NODE_TABLE_ENTRY);
        nodeEntries.push_back(entry);
        nodeTableIDSet.insert(entry->getTableID());
    }
    std::vector<TableCatalogEntry*> relEntries;
    for (auto& name : relTableNames) {
        auto entry = catalog->getTableCatalogEntry(transaction, name, false /* useInternal */);
        validateEntryType(*entry, CatalogEntryType::REL_TABLE_ENTRY);
        validateRelSrcDstNodeAreProjected(*entry, nodeTableIDSet, catalog, transaction);
        relEntries.push_back(entry);
    }
    return std::make_unique<CreateProjectGraphBindData>(graphName, nodeEntries, relEntries);
}

function_set CreateProjectGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::LIST, LogicalTypeID::LIST});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = TableFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
