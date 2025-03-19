#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/value/nested.h"
#include "function/gds/gds.h"
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
    auto entry = graph::ParsedGraphEntry();
    entry.nodeInfos = bindData->nodeInfos;
    entry.relInfos = bindData->relInfos;
    // bind graph entry to check if input is valid or not. Ignore bind result.
    GDSFunction::bindGraphEntry(*input.context->clientContext, entry);
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

static std::string getPredicateStr(Value& val) {
    auto& type = val.getDataType();
    if (type.getLogicalTypeID() != LogicalTypeID::STRUCT) {
        throw BinderException(stringFormat("{} has data type {}. STRUCT was expected.",
            val.toString(), type.toString()));
    }
    for (auto i = 0u; i < StructType::getNumFields(type); i++) {
        auto fieldName = StructType::getField(type, i).getName();
        if (StringUtils::getUpper(fieldName) == "FILTER") {
            const auto childVal = NestedVal::getChildVal(&val, i);
            childVal->validateType(LogicalTypeID::STRING);
            return childVal->getValue<std::string>();
        } else {
            throw BinderException(stringFormat(
                "Unrecognized configuration {}. Supported configuration is 'filter'.", fieldName));
        }
    }
    return {};
}

static std::vector<GraphEntryTableInfo> extractGraphEntryTableInfos(const Value& value) {
    std::vector<GraphEntryTableInfo> infos;
    switch (value.getDataType().getLogicalTypeID()) {
    case LogicalTypeID::LIST: {
        for (auto name : getAsStringVector(value)) {
            infos.emplace_back(name, "" /* empty predicate */);
        }
    } break;
    case LogicalTypeID::STRUCT: {
        for (auto i = 0u; i < StructType::getNumFields(value.getDataType()); ++i) {
            auto& field = StructType::getField(value.getDataType(), i);
            infos.emplace_back(field.getName(),
                getPredicateStr(*NestedVal::getChildVal(&value, i)));
        }
    } break;
    default:
        throw BinderException(
            stringFormat("Input argument {} has data type {}. STRUCT or LIST was expected.",
                value.toString(), value.getDataType().toString()));
    }
    return infos;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const main::ClientContext*,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto bindData = std::make_unique<CreateProjectedGraphBindData>(graphName);
    auto argNode = input->getValue(1);
    bindData->nodeInfos = extractGraphEntryTableInfos(argNode);
    auto argRel = input->getValue(2);
    bindData->relInfos = extractGraphEntryTableInfos(argRel);
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
