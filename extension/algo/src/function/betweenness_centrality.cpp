#include "binder/binder.h"
#include "binder/expression/node_expression.h"
#include "function/algo_function.h"
#include "function/config/max_iterations_config.h"
#include "function/gds/gds.h"
#include "function/table/table_function.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

struct BetweennessCentralityOptionalParams final : public MaxIterationOptionalParams {
    explicit BetweennessCentralityOptionalParams(const expression_vector& optionalParams);

    explicit BetweennessCentralityOptionalParams(OptionalParam<MaxIterations> maxIterations) 
        : MaxIterationOptionalParams{maxIterations} {}

    void evaluateParams(main::ClientContext* context) override {
        MaxIterationOptionalParams::evaluateParams(context);
    }

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<BetweennessCentralityOptionalParams>(maxIterations);
    }
};

struct BetweennessCentralityBindData final : public GDSBindData {
    BetweennessCentralityBindData(expression_vector columns, graph::NativeGraphEntry graphEntry,
        std::shared_ptr<Expression> nodeOutput,
        std::unique_ptr<BetweennessCentralityOptionalParams> optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), std::move(nodeOutput)} {
        this->optionalParams = std::move(optionalParams);
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<BetweennessCentralityBindData>(*this);
    }
};

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    (void)input;
    return 0;
}

static constexpr char BETWEENNESS_CENTRALITY_SCORE[] = "betweeness_centrality_score";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(BETWEENNESS_CENTRALITY_SCORE, LogicalType::DOUBLE()));
    return std::make_unique<BetweennessCentralityBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
        std::make_unique<BetweennessCentralityOptionalParams>(input->optionalParamsLegacy));

    (void)BETWEENNESS_CENTRALITY_SCORE;
    (void)context;
    (void)input;
    return nullptr;
}

function_set BetweennessCentrality::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(BetweennessCentrality::name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = GDSFunction::getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

}
}
