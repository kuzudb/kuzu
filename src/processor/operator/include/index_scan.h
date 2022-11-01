#pragma once

#include "physical_operator.h"
#include "source_operator.h"

#include "src/storage/index/include/hash_index.h"

namespace graphflow {
namespace processor {

// Index scan does not run in parallel. So there is no shared state.
class IndexScan : public PhysicalOperator, public SourceOperator {
public:
    IndexScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor, table_id_t tableID,
        HashIndex* hashIndex, const Literal& hashKey, const DataPos& outDataPos, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{std::move(resultSetDescriptor)},
          tableID{tableID}, hashIndex{hashIndex}, hashKey{hashKey}, outDataPos{outDataPos} {}

    PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::INDEX_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IndexScan>(
            resultSetDescriptor->copy(), tableID, hashIndex, hashKey, outDataPos, id, paramsString);
    }

private:
    table_id_t tableID;
    HashIndex* hashIndex;
    Literal hashKey;
    DataPos outDataPos;

    bool hasExecuted = false;
    shared_ptr<ValueVector> outVector;
};

} // namespace processor
} // namespace graphflow
