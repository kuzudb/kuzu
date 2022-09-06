#pragma once

#include <mutex>

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/storage/store/include/node_table.h"

#define ENABLE_NODE_MASK true

namespace graphflow {
namespace processor {

struct SemiMask {
public:
    SemiMask(node_offset_t maxNodeOffset, uint64_t maxMorselIdx) {
        morselMask = make_unique<bool[]>(maxMorselIdx + 1);
        fill(morselMask.get(), morselMask.get() + maxMorselIdx + 1, false);
#if ENABLE_NODE_MASK
        nodeMask = make_unique<bool[]>(maxNodeOffset + 1);
        fill(nodeMask.get(), nodeMask.get() + maxNodeOffset + 1, false);
#endif
    }

    unique_ptr<bool[]> morselMask;
#if ENABLE_NODE_MASK
    unique_ptr<bool[]> nodeMask;
#endif
};

class ScanNodeIDSharedState {
public:
    explicit ScanNodeIDSharedState(NodesMetadata* nodesMetadata, label_t labelID)
        : initialized{false}, nodesMetadata{nodesMetadata}, labelID{labelID},
          maxNodeOffset{UINT64_MAX}, maxMorselIdx{UINT64_MAX}, currentNodeOffset{0} {}

    void initialize(Transaction* transaction, node_offset_t filter);

    pair<uint64_t, uint64_t> getNextRangeToRead();

    void initSemiMask(Transaction* transaction);

    // Notice: This function is not protected with a lock for concurrent writes because of the
    // special use case that there is no mixed reads and writes to the mask, and all writes to the
    // mask try to set a position to true, thus it doesn't matter which thread succeeds.
    inline void setMask(uint64_t nodeOffset) {
        auto morselIdx = nodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2;
        if (!mask->morselMask[morselIdx]) {
            mask->morselMask[morselIdx] = true;
        }
#if ENABLE_NODE_MASK
        if (!mask->nodeMask[nodeOffset]) {
            mask->nodeMask[nodeOffset] = true;
        }
#endif
    }
    inline bool isSemiMaskEnabled() const { return mask != nullptr; }
#if ENABLE_NODE_MASK
    inline bool isNodeMasked(node_offset_t nodeOffset) const { return mask->nodeMask[nodeOffset]; }
#endif

private:
    mutex mtx;
    bool initialized;
    NodesMetadata* nodesMetadata;
    label_t labelID;
    uint64_t maxNodeOffset;
    uint64_t maxMorselIdx;
    uint64_t currentNodeOffset;
    unique_ptr<SemiMask> mask;
};

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(string nodeID, node_offset_t filter,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, NodeTable* nodeTable,
        const DataPos& outDataPos, shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{std::move(resultSetDescriptor)},
          nodeID{move(nodeID)}, filter{filter}, nodeTable{nodeTable}, outDataPos{outDataPos},
          sharedState{std::move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return SCAN_NODE_ID; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline ScanNodeIDSharedState* getSharedState() const { return sharedState.get(); }

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID>(nodeID, filter, resultSetDescriptor->copy(), nodeTable,
            outDataPos, sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    void setSelVector(node_offset_t startOffset, node_offset_t endOffset);

public:
    string nodeID;

private:
    node_offset_t filter;
    NodeTable* nodeTable;
    DataPos outDataPos;
    shared_ptr<ScanNodeIDSharedState> sharedState;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
