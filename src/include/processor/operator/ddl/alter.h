#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

struct AlterPrintInfo final : OPPrintInfo {
    common::AlterType alterType;
    std::string tableName;
    binder::BoundAlterInfo info;

    AlterPrintInfo(common::AlterType alterType, std::string tableName, binder::BoundAlterInfo info)
        : alterType{std::move(alterType)}, tableName{std::move(tableName)}, info{std::move(info)} {}

    std::string toString() const override { return info.toString(); }

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<AlterPrintInfo>(new AlterPrintInfo(*this));
    }

private:
    AlterPrintInfo(const AlterPrintInfo& other)
        : OPPrintInfo{other}, alterType{other.alterType}, tableName{other.tableName},
          info{other.info.copy()} {}
};

class Alter final : public DDL {
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

    void executeDDLInternal(ExecutionContext* context) override;

    std::string getOutputMsg() override;

    std::unique_ptr<PhysicalOperator> clone() override {
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
