#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Filter : public PhysicalOperator, public SelVectorOverWriter {
public:
    Filter(std::unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator,
        uint32_t dataChunkToSelectPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FILTER, std::move(child), id, paramsString},
          expressionEvaluator{std::move(expressionEvaluator)},
          dataChunkToSelectPos(dataChunkToSelectPos) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Filter>(expressionEvaluator->clone(), dataChunkToSelectPos,
            children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<evaluator::BaseExpressionEvaluator> expressionEvaluator;
    uint32_t dataChunkToSelectPos;
    std::shared_ptr<common::DataChunk> dataChunkToSelect;
};

} // namespace processor
} // namespace kuzu
