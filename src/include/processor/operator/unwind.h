#pragma once

#include "binder/expression/expression.h"
#include "common/vector/value_vector_utils.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class Unwind : public PhysicalOperator {
public:
    Unwind(DataType outDataType, DataPos outDataPos,
        unique_ptr<BaseExpressionEvaluator> expressionEvaluator, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::UNWIND, std::move(child), id, paramsString},
          outDataType{std::move(outDataType)}, outDataPos{outDataPos},
          expressionEvaluator{std::move(expressionEvaluator)}, startIndex{0u} {}

    bool getNextTuplesInternal() override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(outDataType, outDataPos, expressionEvaluator->clone(),
            children[0]->clone(), id, paramsString);
    }

private:
    bool hasMoreToRead() const;
    void copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const;

    DataType outDataType;
    DataPos outDataPos;

    unique_ptr<BaseExpressionEvaluator> expressionEvaluator;
    shared_ptr<ValueVector> outValueVector;
    uint32_t startIndex;
    ku_list_t inputList;
};

} // namespace processor
} // namespace kuzu
