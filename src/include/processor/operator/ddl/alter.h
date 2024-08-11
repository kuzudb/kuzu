#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "planner/operator/ddl/logical_alter.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

struct AlterPrintInfo final : OPPrintInfo {
    std::unique_ptr<BaseAlterPrintInfo> base;

    AlterPrintInfo(std::unique_ptr<BaseAlterPrintInfo> base)
        : base(std::move(base)) {}
    
    std::string toString() const override {return base->toString();};

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<AlterPrintInfo>(new AlterPrintInfo(*this));
    }

private:
    AlterPrintInfo(const AlterPrintInfo& other)
        : OPPrintInfo{other}, base(std::make_unique<BaseAlterPrintInfo>(*other.base)){}
};

class Alter : public DDL {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::ALTER;

public:
    Alter(binder::BoundAlterInfo info,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        const DataPos& outputPos, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : DDL{type_, outputPos, id, std::move(printInfo)}, info{std::move(info)},
          defaultValueEvaluator{std::move(defaultValueEvaluator)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        DDL::initLocalStateInternal(resultSet, context);
        if (defaultValueEvaluator) {
            defaultValueEvaluator->init(*resultSet, context->clientContext);
        }
    }

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<Alter>(info.copy(),
            defaultValueEvaluator == nullptr ? nullptr : defaultValueEvaluator->clone(), outputPos,
            id, printInfo->copy());
    }

private:
    binder::BoundAlterInfo info;
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
};

} // namespace processor
} // namespace kuzu
