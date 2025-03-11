#pragma once

#include "function/gds/rec_joins.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::RECURSIVE_EXTEND;

public:
    LogicalRecursiveExtend(std::unique_ptr<function::RJAlgorithm> function,
        function::RJBindData bindData, binder::expression_vector resultColumns,
        common::table_id_set_t nbrTableIDSet, std::shared_ptr<LogicalOperator> nodeMaskRoot)
        : LogicalOperator{operatorType_}, function{std::move(function)}, bindData{bindData},
          resultColumns{std::move(resultColumns)}, nbrTableIDSet{std::move(nbrTableIDSet)},
          limitNum{common::INVALID_LIMIT}, nodeMaskRoot{std::move(nodeMaskRoot)} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    void setFunction(std::unique_ptr<function::RJAlgorithm> func) { function = std::move(func); }
    const function::RJAlgorithm& getFunction() const { return *function; }

    const function::RJBindData& getBindData() const { return bindData; }
    function::RJBindData& getBindDataUnsafe() { return bindData; }

    void setResultColumns(binder::expression_vector exprs) { resultColumns = std::move(exprs); }
    binder::expression_vector getResultColumns() const { return resultColumns; }

    bool hasNbrTableIDSet() const { return !nbrTableIDSet.empty(); }
    common::table_id_set_t getNbrTableIDSet() const { return nbrTableIDSet; }

    void setLimitNum(common::offset_t num) { limitNum = num; }
    common::offset_t getLimitNum() const { return limitNum; }

    bool hasNodePredicate() const { return nodeMaskRoot != nullptr; }
    std::shared_ptr<LogicalOperator> getNodeMaskRoot() const { return nodeMaskRoot; }

    std::string getExpressionsForPrinting() const override { return function->getFunctionName(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(function->copy(), bindData, resultColumns,
            nbrTableIDSet, nodeMaskRoot);
    }

private:
    std::unique_ptr<function::RJAlgorithm> function;
    function::RJBindData bindData;
    binder::expression_vector resultColumns;

    common::table_id_set_t nbrTableIDSet;
    common::offset_t limitNum; // TODO: remove this once recursive extend is pipelined.
    std::shared_ptr<LogicalOperator> nodeMaskRoot;
};

} // namespace planner
} // namespace kuzu
