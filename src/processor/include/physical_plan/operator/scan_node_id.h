#pragma once

#include <mutex>

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace processor {

struct SemiMask {
public:
    SemiMask(node_offset_t maxNodeOffset, uint64_t maxMorselIdx) {
        morselMask = make_unique<bool[]>(maxMorselIdx + 1);
        nodeMask = make_unique<bool[]>(maxNodeOffset + 1);
        fill(morselMask.get(), morselMask.get() + maxMorselIdx + 1, false);
        fill(nodeMask.get(), nodeMask.get() + maxNodeOffset + 1, false);
    }

    unique_ptr<bool[]> morselMask;
    unique_ptr<bool[]> nodeMask;
};

class ScanNodeIDSharedState {

public:
    explicit ScanNodeIDSharedState(
        NodesMetadata* nodesMetadata, table_id_t tableID, bool enableSemiMask = false)
        : initialized{false}, enableSemiMask{enableSemiMask}, nodesMetadata{nodesMetadata},
          tableID{tableID}, maxNodeOffset{UINT64_MAX}, maxMorselIdx{UINT64_MAX}, currentNodeOffset{
                                                                                     0} {}

    void initialize(Transaction* transaction);

    pair<uint64_t, uint64_t> getNextRangeToRead();

    // Notice: This function is not protected with a lock for concurrent writes because of the
    // special use case that there is no mixed reads and writes to the mask, and all writes to the
    // mask try to set a position to true, thus it doesn't matter which thread succeeds.
    inline void setMask(uint64_t nodeOffset) {
        mask->morselMask[nodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2] = true;
        mask->nodeMask[nodeOffset] = true;
    }
    inline bool isSemiMaskEnabled() const { return enableSemiMask; }
    inline bool isNodeMasked(node_offset_t nodeOffset) const { return mask->nodeMask[nodeOffset]; }

private:
    mutex mtx;
    bool initialized;
    bool enableSemiMask;
    NodesMetadata* nodesMetadata;
    table_id_t tableID;
    uint64_t maxNodeOffset;
    uint64_t maxMorselIdx;
    uint64_t currentNodeOffset;
    unique_ptr<SemiMask> mask;
};

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(unique_ptr<ResultSetDescriptor> resultSetDescriptor, NodeTable* nodeTable,
        const DataPos& outDataPos, shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{std::move(resultSetDescriptor)},
          nodeTable{nodeTable}, outDataPos{outDataPos}, sharedState{std::move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return SCAN_NODE_ID; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID>(
            resultSetDescriptor->copy(), nodeTable, outDataPos, sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    void setSelVector(node_offset_t startOffset, node_offset_t endOffset);

private:
    NodeTable* nodeTable;
    DataPos outDataPos;
    shared_ptr<ScanNodeIDSharedState> sharedState;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
