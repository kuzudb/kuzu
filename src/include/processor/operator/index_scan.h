#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "physical_operator.h"
#include "source_operator.h"
#include "storage/index/hash_index.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

// Index scan does not run in parallel. So there is no shared state.
class IndexScan : public PhysicalOperator, public SourceOperator {
public:
    IndexScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor, table_id_t tableID,
        PrimaryKeyIndex* pkIndex, unique_ptr<BaseExpressionEvaluator> indexKeyEvaluator,
        const DataPos& outDataPos, uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString},
          SourceOperator{std::move(resultSetDescriptor)}, tableID{tableID}, pkIndex{pkIndex},
          indexKeyEvaluator{std::move(indexKeyEvaluator)}, outDataPos{outDataPos} {}

    PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::INDEX_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IndexScan>(resultSetDescriptor->copy(), tableID, pkIndex,
            indexKeyEvaluator->clone(), outDataPos, id, paramsString);
    }

private:
    table_id_t tableID;
    PrimaryKeyIndex* pkIndex;
    unique_ptr<BaseExpressionEvaluator> indexKeyEvaluator;
    DataPos outDataPos;

    bool hasExecuted = false;
    shared_ptr<ValueVector> outVector;
};

} // namespace processor
} // namespace kuzu
