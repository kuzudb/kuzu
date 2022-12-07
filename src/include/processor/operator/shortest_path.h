#pragma once

#include "binder/expression/expression.h"
#include "common/vector/value_vector_utils.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/source_operator.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class ShortestPath : public PhysicalOperator {
public:
    ShortestPath(unique_ptr<BaseExpressionEvaluator> srcNodeExprEvaluator,
        unique_ptr<BaseExpressionEvaluator> destNodeExprEvaluator,
        unordered_set<table_id_t> tableIDs, uint64_t lowerBound, uint64_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, srcNodeExprEvaluator{move(
                                                               srcNodeExprEvaluator)},
          destNodeExprEvaluator{move(destNodeExprEvaluator)}, tableIDs{move(tableIDs)},
          lowerBound{lowerBound}, upperBound{upperBound} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SHORTEST_PATH;
    }

    bool getNextTuplesInternal() override;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ShortestPath>(srcNodeExprEvaluator->clone(),
            destNodeExprEvaluator->clone(), tableIDs, lowerBound, upperBound, children[0]->clone(),
            id, paramsString);
    }

private:
    unique_ptr<BaseExpressionEvaluator> srcNodeExprEvaluator;
    unique_ptr<BaseExpressionEvaluator> destNodeExprEvaluator;
    unordered_set<table_id_t> tableIDs;
    uint64_t lowerBound;
    uint64_t upperBound;
};

} // namespace processor
} // namespace kuzu