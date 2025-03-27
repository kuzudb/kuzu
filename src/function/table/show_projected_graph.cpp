#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct ProjectedGraphData {
    std::string name;
    std::string nodeInfo;
    std::string relInfo;

    ProjectedGraphData(std::string name, std::string nodeInfo, std::string relInfo)
        : name{std::move(name)}, nodeInfo{std::move(nodeInfo)}, relInfo{std::move(relInfo)} {}
};

struct ShowProjectedGraphBindData : public TableFuncBindData {
    std::vector<ProjectedGraphData> projectedGraphData;

    ShowProjectedGraphBindData(std::vector<ProjectedGraphData> projectedGraphData,
        binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), projectedGraphData.size()},
          projectedGraphData{std::move(projectedGraphData)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowProjectedGraphBindData>(projectedGraphData, columns);
    }
};

static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
    DataChunk& output) {
    auto& projectedGraphData =
        input.bindData->constPtrCast<ShowProjectedGraphBindData>()->projectedGraphData;
    auto numTablesToOutput = morsel.endOffset - morsel.startOffset;
    for (auto i = 0u; i < numTablesToOutput; i++) {
        auto graphData = projectedGraphData[morsel.startOffset + i];
        output.getValueVectorMutable(0).setValue(i, graphData.name);
        output.getValueVectorMutable(1).setValue(i, graphData.nodeInfo);
        output.getValueVectorMutable(2).setValue(i, graphData.relInfo);
    }
    return numTablesToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    std::vector<std::string> returnColumnNames;
    std::vector<LogicalType> returnTypes;
    returnColumnNames.emplace_back("name");
    returnTypes.emplace_back(LogicalType::STRING());
    returnColumnNames.emplace_back("nodes");
    returnTypes.emplace_back(LogicalType::STRING());
    returnColumnNames.emplace_back("rels");
    returnTypes.emplace_back(LogicalType::STRING());
    returnColumnNames =
        TableFunction::extractYieldVariables(returnColumnNames, input->yieldVariables);
    auto columns = input->binder->createVariables(returnColumnNames, returnTypes);
    std::vector<ProjectedGraphData> projectedGraphData;
    for (auto& [name, parsedGraphEntry] : context->getGraphEntrySet()) {
        std::string nodeInfo = "{", relInfo = "{";
        for (auto i = 0u; i < parsedGraphEntry.nodeInfos.size() - 1; i++) {
            nodeInfo += parsedGraphEntry.nodeInfos[i].toString();
            nodeInfo += ",";
        }
        nodeInfo += parsedGraphEntry.nodeInfos[parsedGraphEntry.nodeInfos.size() - 1].toString();
        nodeInfo += "}";
        for (auto i = 0u; i < parsedGraphEntry.relInfos.size() - 1; i++) {
            relInfo += parsedGraphEntry.relInfos[i].toString();
            relInfo += ",";
        }
        relInfo += parsedGraphEntry.relInfos[parsedGraphEntry.relInfos.size() - 1].toString();
        relInfo += "}";
        projectedGraphData.emplace_back(name, std::move(nodeInfo), std::move(relInfo));
    }
    return std::make_unique<ShowProjectedGraphBindData>(std::move(projectedGraphData),
        std::move(columns));
}

function_set ShowProjectedGraphFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
