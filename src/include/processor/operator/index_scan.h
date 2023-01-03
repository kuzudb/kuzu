#pragma once

#include "expression_evaluator/base_evaluator.h"
#include "physical_operator.h"
#include "processor/operator/filtering_operator.h"
#include "storage/index/hash_index.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

// Index scan does not run in parallel. So there is no shared state.
class IndexScan : public PhysicalOperator, public SelVectorOverWriter {
public:
    IndexScan(table_id_t tableID, PrimaryKeyIndex* pkIndex, const DataPos& indexDataPos,
        const DataPos& outDataPos, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INDEX_SCAN, std::move(child), id, paramsString},
          tableID{tableID}, pkIndex{pkIndex}, indexDataPos{indexDataPos}, outDataPos{outDataPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IndexScan>(
            tableID, pkIndex, indexDataPos, outDataPos, children[0]->clone(), id, paramsString);
    }

private:
    table_id_t tableID;
    PrimaryKeyIndex* pkIndex;
    DataPos indexDataPos;
    DataPos outDataPos;

    shared_ptr<ValueVector> indexVector;
    shared_ptr<ValueVector> outVector;
};

} // namespace processor
} // namespace kuzu
