#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/value/nested.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"
#include "graph/graph_entry.h"
#include "parser/parser.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct CreateProjectedGraphBindData final : TableFuncBindData {
    std::string graphName;
    std::vector<GraphEntryTableInfo> nodeInfos;
    std::vector<GraphEntryTableInfo> relInfos;

    explicit CreateProjectedGraphBindData(std::string graphName)
        : graphName{std::move(graphName)} {}
    CreateProjectedGraphBindData(std::string graphName, std::vector<GraphEntryTableInfo> nodeInfos,
        std::vector<GraphEntryTableInfo> relInfos)
        : TableFuncBindData{0}, graphName{std::move(graphName)}, nodeInfos{std::move(nodeInfos)},
          relInfos{std::move(relInfos)} {}

    void addNodeInfo(TableCatalogEntry* entry) { nodeInfos.emplace_back(entry); }
    void addNodeInfo(TableCatalogEntry* entry, std::shared_ptr<Expression> predicate) {
        nodeInfos.emplace_back(entry, predicate);
    }

    void addRelInfo(TableCatalogEntry* entry) { relInfos.emplace_back(entry); }
    void addRelInfo(TableCatalogEntry* entry, std::shared_ptr<Expression> predicate) {
        relInfos.emplace_back(entry, std::move(predicate));
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CreateProjectedGraphBindData>(graphName, nodeInfos, relInfos);
    }
};

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto bindData = ku_dynamic_cast<CreateProjectedGraphBindData*>(input.bindData);
    auto& graphEntrySet = input.context->clientContext->getGraphEntrySetUnsafe();
    if (graphEntrySet.hasGraph(bindData->graphName)) {
        throw RuntimeException(
            stringFormat("Project graph {} already exists.", bindData->graphName));
    }
    auto entry = graph::GraphEntry();
    entry.nodeInfos = bindData->nodeInfos;
    entry.relInfos = bindData->relInfos;
    graphEntrySet.addGraph(bindData->graphName, entry);
    return 0;
}

static std::vector<std::string> getAsStringVector(const Value& value) {
    std::vector<std::string> result;
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
        auto childVal = NestedVal::getChildVal(&value, i);
        childVal->validateType(LogicalTypeID::STRING);
        result.push_back(childVal->getValue<std::string>());
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

static std::shared_ptr<Expression> getFilterField(Value& val, const std::string& cypherTemplate, Binder* binder) {
    auto& type = val.getDataType();
    if (type.getLogicalTypeID() != LogicalTypeID::STRUCT) {
        throw BinderException(stringFormat("{} has data type {}. STRUCT was expected.",
            val.toString(), type.toString()));
    }
    for (auto j = 0u; j < StructType::getNumFields(type); ++j) {
        auto fieldName = StructType::getField(type, j).getName();
        if (StringUtils::getUpper(fieldName) == "FILTER") {
            auto childVal = NestedVal::getChildVal(&val, j);
            childVal->validateType(LogicalTypeID::STRING);
            auto predicateStr = childVal->getValue<std::string>();
            auto statementStr = stringFormat(cypherTemplate, predicateStr);
            auto parsedStatements = parser::Parser::parseQuery(statementStr);
            KU_ASSERT(parsedStatements.size() == 1);
            auto boundStatement = binder->bind(*parsedStatements[0]);
            return boundStatement->getStatementResult()->getColumns()[0];
        } else {
            throw BinderException(
                stringFormat("Unrecognized configuration {}.", fieldName));
        }
    }
    return nullptr;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    // Bind graph name
    auto graphName = input->getLiteralVal<std::string>(0);
    auto bindData = std::make_unique<CreateProjectedGraphBindData>(graphName);
    // Bind node table
    auto arg2 = input->getValue(1);
    common::table_id_set_t nodeTableIDSet;
    switch (arg2.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        for (auto& name : getAsStringVector(arg2)) {
            auto entry = catalog->getTableCatalogEntry(transaction, name, false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::NODE_TABLE_ENTRY);
            bindData->addNodeInfo(entry);
            nodeTableIDSet.insert(entry->getTableID());
        }
    } break;
    case LogicalTypeID::STRUCT: {
        for (auto i = 0u; i < StructType::getNumFields(arg2.getDataType()); ++i) {
            auto& field = StructType::getField(arg2.getDataType(), i);
            auto entry = catalog->getTableCatalogEntry(transaction, field.getName(),
                false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::NODE_TABLE_ENTRY);
            auto childVal = NestedVal::getChildVal(&arg2, i);
            auto matchPattern = stringFormat("MATCH (n:{}) ", entry->getName());
            auto predicate = getFilterField(*childVal, matchPattern + " RETURN {}", input->binder);
            bindData->addNodeInfo(entry, predicate);
            nodeTableIDSet.insert(entry->getTableID());
        }
    } break;
    default:
        throw BinderException(stringFormat("{} has data type {}. STRUCT or LIST was expected.",
            arg2.toString(), arg2.getDataType().toString()));
    }
    // Bind rel table
    auto arg3 = input->getValue(2);
    switch (arg3.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        for (auto& name : getAsStringVector(arg3)) {
            auto entry = catalog->getTableCatalogEntry(transaction, name, false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::REL_TABLE_ENTRY);
            validateRelSrcDstNodeAreProjected(*entry, nodeTableIDSet, catalog, transaction);
            bindData->addRelInfo(entry);
        }
    } break;
    case LogicalTypeID::STRUCT: {
        for (auto i = 0u; i < StructType::getNumFields(arg3.getDataType()); ++i) {
            auto& field = StructType::getField(arg3.getDataType(), i);
            auto entry = catalog->getTableCatalogEntry(transaction, field.getName(),
                false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::REL_TABLE_ENTRY);
            validateRelSrcDstNodeAreProjected(*entry, nodeTableIDSet, catalog, transaction);
            auto childVal = NestedVal::getChildVal(&arg3, i);
            auto matchPattern = stringFormat("MATCH ()-[r:{}]->() ", entry->getName());
            auto predicate = getFilterField(*childVal, matchPattern + " RETURN {}", input->binder);
            bindData->addRelInfo(entry, predicate);
        }
    } break;
    default:
        throw BinderException(stringFormat("{} has data type {}. STRUCT or LIST was expected.",
            arg3.toString(), arg3.getDataType().toString()));
    }
    return bindData;
}

function_set CreateProjectedGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::LIST, LogicalTypeID::ANY});
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
