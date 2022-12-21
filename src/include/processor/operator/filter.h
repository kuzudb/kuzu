#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class Filter : public PhysicalOperator, public SelVectorOverWriter {
public:
    Filter(unique_ptr<BaseExpressionEvaluator> expressionEvaluator, uint32_t dataChunkToSelectPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FILTER, std::move(child), id, paramsString},
          expressionEvaluator{std::move(expressionEvaluator)},
          dataChunkToSelectPos(dataChunkToSelectPos) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Filter>(expressionEvaluator->clone(), dataChunkToSelectPos,
            children[0]->clone(), id, paramsString);
    }

private:
    unique_ptr<BaseExpressionEvaluator> expressionEvaluator;
    uint32_t dataChunkToSelectPos;
    shared_ptr<DataChunk> dataChunkToSelect;
};

} // namespace processor
} // namespace kuzu
