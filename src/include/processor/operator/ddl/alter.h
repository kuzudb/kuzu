#pragma once

#include "binder/ddl/bound_alter_info.h"
#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/ddl/ddl.h"

namespace kuzu {
namespace processor {

class Alter : public DDL {
public:
    Alter(binder::BoundAlterInfo info,
        std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::ALTER, outputPos, id, paramsString}, info{std::move(info)},
          defaultValueEvaluator{std::move(defaultValueEvaluator)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        DDL::initLocalStateInternal(resultSet, context);
        if (defaultValueEvaluator) {
            defaultValueEvaluator->init(*resultSet, context->clientContext->getMemoryManager());
        }
    }

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<Alter>(info.copy(),
            defaultValueEvaluator == nullptr ? nullptr : defaultValueEvaluator->clone(), outputPos,
            id, paramsString);
    }

private:
    binder::BoundAlterInfo info;
    std::unique_ptr<evaluator::ExpressionEvaluator> defaultValueEvaluator;
};

} // namespace processor
} // namespace kuzu
