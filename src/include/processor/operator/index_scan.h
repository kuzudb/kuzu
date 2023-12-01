#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "physical_operator.h"
#include "processor/operator/filtering_operator.h"
#include "storage/index/hash_index.h"

namespace kuzu {
namespace processor {

// Index scan does not run in parallel. So there is no shared state.
class IndexScan : public PhysicalOperator, public SelVectorOverWriter {
public:
    IndexScan(common::table_id_t tableID, storage::PrimaryKeyIndex* pkIndex,
        std::unique_ptr<evaluator::ExpressionEvaluator> indexEvaluator, const DataPos& outDataPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INDEX_SCAN, std::move(child), id, paramsString},
          tableID{tableID}, pkIndex{pkIndex}, indexEvaluator{std::move(indexEvaluator)},
          outDataPos{outDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IndexScan>(tableID, pkIndex, indexEvaluator->clone(), outDataPos,
            children[0]->clone(), id, paramsString);
    }

private:
    common::table_id_t tableID;
    storage::PrimaryKeyIndex* pkIndex;
    std::unique_ptr<evaluator::ExpressionEvaluator> indexEvaluator;
    DataPos outDataPos;

    common::ValueVector* indexVector;
    common::ValueVector* outVector;
};

} // namespace processor
} // namespace kuzu
