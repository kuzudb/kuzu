#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class Unwind : public PhysicalOperator {
public:
    Unwind(common::LogicalType outDataType, DataPos outDataPos,
        std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::UNWIND, std::move(child), id, paramsString},
          outDataType{std::move(outDataType)}, outDataPos{outDataPos},
          expressionEvaluator{std::move(expressionEvaluator)}, startIndex{0u} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(outDataType, outDataPos, expressionEvaluator->clone(),
            children[0]->clone(), id, paramsString);
    }

private:
    bool hasMoreToRead() const;
    void copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const;

    common::LogicalType outDataType;
    DataPos outDataPos;

    std::unique_ptr<evaluator::ExpressionEvaluator> expressionEvaluator;
    std::shared_ptr<common::ValueVector> outValueVector;
    uint32_t startIndex;
    common::list_entry_t listEntry;
};

} // namespace processor
} // namespace kuzu
