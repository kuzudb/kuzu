#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/value/nested.h"
#include "function/table/bind_data.h"
#include "function/table/standalone_call_function.h"
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
    void addNodeInfo(TableCatalogEntry* entry, std::shared_ptr<Expression> node,
        std::shared_ptr<Expression> predicate) {
        nodeInfos.emplace_back(entry, std::move(node), std::move(predicate));
    }

    void addRelInfo(TableCatalogEntry* entry) { relInfos.emplace_back(entry); }
    void addRelInfo(TableCatalogEntry* entry, std::shared_ptr<Expression> rel,
        std::shared_ptr<Expression> predicate) {
        relInfos.emplace_back(entry, std::move(rel), std::move(predicate));
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

static expression_pair getFilterField(Value& val, const std::string& cypherTemplate,
    Binder* binder) {
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
            std::string statementStr;
            if (!predicateStr.empty()) {
                statementStr = stringFormat(cypherTemplate + ", {}", predicateStr);
            } else {
                statementStr = cypherTemplate;
            }
            auto parsedStatements = parser::Parser::parseQuery(statementStr);
            KU_ASSERT(parsedStatements.size() == 1);
            auto boundStatement = binder->bind(*parsedStatements[0]);
            auto columns = boundStatement->getStatementResult()->getColumns();
            if (columns.size() == 1) {
                return {columns[0], nullptr};
            } else {
                KU_ASSERT(columns.size() == 2);
                return {columns[0], columns[1]};
            }
        } else {
            throw BinderException(stringFormat(
                "Unrecognized configuration {}. Supported configuration is 'filter'.", fieldName));
        }
    }
    return {};
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    // Bind graph name
    auto graphName = input->getLiteralVal<std::string>(0);
    auto bindData = std::make_unique<CreateProjectedGraphBindData>(graphName);
    // Bind node table
    auto argNode = input->getValue(1);
    common::table_id_set_t nodeTableIDSet;
    switch (argNode.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        for (auto& name : getAsStringVector(argNode)) {
            auto entry = catalog->getTableCatalogEntry(transaction, name, false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::NODE_TABLE_ENTRY);
            bindData->addNodeInfo(entry);
            nodeTableIDSet.insert(entry->getTableID());
        }
    } break;
    case LogicalTypeID::STRUCT: {
        for (auto i = 0u; i < StructType::getNumFields(argNode.getDataType()); ++i) {
            auto& field = StructType::getField(argNode.getDataType(), i);
            auto entry = catalog->getTableCatalogEntry(transaction, field.getName(),
                false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::NODE_TABLE_ENTRY);
            auto matchPattern = stringFormat("MATCH (n:{}) ", entry->getName());
            auto [node, predicate] = getFilterField(*NestedVal::getChildVal(&argNode, i),
                matchPattern + " RETURN n", input->binder);
            bindData->addNodeInfo(entry, node, predicate);
            nodeTableIDSet.insert(entry->getTableID());
        }
    } break;
    default:
        throw BinderException(
            stringFormat("Input argument {} has data type {}. STRUCT or LIST was expected.",
                argNode.toString(), argNode.getDataType().toString()));
    }
    // Bind rel table
    auto argRel = input->getValue(2);
    switch (argRel.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        for (auto& name : getAsStringVector(argRel)) {
            auto entry = catalog->getTableCatalogEntry(transaction, name, false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::REL_TABLE_ENTRY);
            validateRelSrcDstNodeAreProjected(*entry, nodeTableIDSet, catalog, transaction);
            bindData->addRelInfo(entry);
        }
    } break;
    case LogicalTypeID::STRUCT: {
        for (auto i = 0u; i < StructType::getNumFields(argRel.getDataType()); ++i) {
            auto& field = StructType::getField(argRel.getDataType(), i);
            auto entry = catalog->getTableCatalogEntry(transaction, field.getName(),
                false /* useInternal */);
            validateEntryType(*entry, CatalogEntryType::REL_TABLE_ENTRY);
            validateRelSrcDstNodeAreProjected(*entry, nodeTableIDSet, catalog, transaction);
            auto matchPattern = stringFormat("MATCH ()-[r:{}]->() ", entry->getName());
            auto [rel, predicate] = getFilterField(*NestedVal::getChildVal(&argRel, i),
                matchPattern + " RETURN r", input->binder);
            bindData->addRelInfo(entry, rel, predicate);
        }
    } break;
    default:
        throw BinderException(
            stringFormat("Input argument {} has data type {}. STRUCT or LIST was expected.",
                argRel.toString(), argRel.getDataType().toString()));
    }
    return bindData;
}

function_set CreateProjectedGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::ANY, LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = TableFunction::initEmptySharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = []() { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
