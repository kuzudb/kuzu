#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "graph/graph_entry_set.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct ProjectedGraphInfo {
    virtual ~ProjectedGraphInfo() = default;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

    virtual std::unique_ptr<ProjectedGraphInfo> copy() const = 0;
};

struct NativeProjectedGraphInfo : public ProjectedGraphInfo {
    std::string nodeInfo;
    std::string relInfo;

    NativeProjectedGraphInfo(std::string nodeInfo, std::string relInfo)
        : nodeInfo{std::move(nodeInfo)}, relInfo{std::move(relInfo)} {}

    std::unique_ptr<ProjectedGraphInfo> copy() const override {
        return std::make_unique<NativeProjectedGraphInfo>(nodeInfo, relInfo);
    }
};

struct CypherProjectedGraphInfo : public ProjectedGraphInfo {
    std::string cypherQuery;

    explicit CypherProjectedGraphInfo(std::string cypherQuery)
        : cypherQuery{std::move(cypherQuery)} {}

    std::unique_ptr<ProjectedGraphInfo> copy() const override {
        return std::make_unique<CypherProjectedGraphInfo>(cypherQuery);
    }
};

struct ProjectedGraphInfoBindData : public TableFuncBindData {
    graph::GraphEntryType type;
    std::unique_ptr<ProjectedGraphInfo> info;

    ProjectedGraphInfoBindData(binder::expression_vector columns, graph::GraphEntryType type,
        std::unique_ptr<ProjectedGraphInfo> info)
        : TableFuncBindData{std::move(columns), 1 /* oneRowResult */}, type{std::move(type)},
          info{std::move(info)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ProjectedGraphInfoBindData>(columns, type, info->copy());
    }
};

static offset_t internalTableFunc(const TableFuncMorsel& /*morsel*/, const TableFuncInput& input,
    DataChunk& output) {
    auto projectedGraphData = input.bindData->constPtrCast<ProjectedGraphInfoBindData>();
    output.getValueVectorMutable(0).setValue(0,
        graph::GraphEntryTypeUtils::toString(projectedGraphData->type));
    switch (projectedGraphData->type) {
    case graph::GraphEntryType::NATIVE: {
        output.getValueVectorMutable(1).setValue(0,
            projectedGraphData->info->constCast<NativeProjectedGraphInfo>().nodeInfo);
        output.getValueVectorMutable(2).setValue(0,
            projectedGraphData->info->constCast<NativeProjectedGraphInfo>().relInfo);
    } break;
    case graph::GraphEntryType::CYPHER: {
        output.getValueVectorMutable(1).setValue(0,
            projectedGraphData->info->constCast<CypherProjectedGraphInfo>().cypherQuery);
    } break;
    default:
        KU_UNREACHABLE;
    }
    return 1;
}

static std::string getNodeOrRelInfo(
    const std::vector<graph::ParsedNativeGraphTableInfo>& tableInfo) {
    if (tableInfo.empty()) {
        return "";
    }
    std::string info = "{";
    for (auto i = 0u; i < tableInfo.size(); i++) {
        info += tableInfo[i].toString();
        if (i == tableInfo.size() - 1) {
            info += "}";
        } else {
            info += ",";
        }
    }
    return info;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("type");
    returnTypes.emplace_back(LogicalType::STRING());
    auto graphEntry = context->getGraphEntrySet().getEntry(input->getValue(0).toString());
    switch (graphEntry->type) {
    case graph::GraphEntryType::CYPHER: {
        returnColumnNames.emplace_back("cypher statement");
        returnTypes.emplace_back(LogicalType::STRING());
    } break;
    case graph::GraphEntryType::NATIVE: {
        returnColumnNames.emplace_back("nodes");
        returnTypes.emplace_back(LogicalType::STRING());
        returnColumnNames.emplace_back("rels");
        returnTypes.emplace_back(LogicalType::STRING());
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    returnColumnNames =
        TableFunction::extractYieldVariables(returnColumnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    std::unique_ptr<ProjectedGraphInfo> projectedGraphInfo;
    switch (graphEntry->type) {
    case graph::GraphEntryType::CYPHER: {
        auto& cypherGraphEntry = graphEntry->cast<graph::ParsedCypherGraphEntry>();
        projectedGraphInfo =
            std::make_unique<CypherProjectedGraphInfo>(cypherGraphEntry.cypherQuery);
    } break;
    case graph::GraphEntryType::NATIVE: {
        auto& nativeGraphEntry = graphEntry->cast<graph::ParsedNativeGraphEntry>();
        projectedGraphInfo =
            std::make_unique<NativeProjectedGraphInfo>(getNodeOrRelInfo(nativeGraphEntry.nodeInfos),
                getNodeOrRelInfo(nativeGraphEntry.relInfos));
    } break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<ProjectedGraphInfoBindData>(std::move(columns), graphEntry->type,
        std::move(projectedGraphInfo));
}

function_set ProjectedGraphInfoFunction::getFunctionSet() {
    function_set functionSet;
    auto function =
        std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
