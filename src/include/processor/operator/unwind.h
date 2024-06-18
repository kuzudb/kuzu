#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class Unwind : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::UNWIND;

public:
    Unwind(DataPos outDataPos, DataPos idPos,
        std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          outDataPos{outDataPos}, idPos(idPos), expressionEvaluator{std::move(expressionEvaluator)},
          startIndex{0u} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(outDataPos, idPos, expressionEvaluator->clone(),
            children[0]->clone(), id, printInfo->copy());
    }

private:
    bool hasMoreToRead() const;
    void copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const;

    DataPos outDataPos;
    DataPos idPos;

    std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator;
    std::shared_ptr<common::ValueVector> outValueVector;
    common::ValueVector* idVector = nullptr;
    uint32_t startIndex;
    common::list_entry_t listEntry;
};

} // namespace processor
} // namespace kuzu
