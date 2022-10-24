#pragma once

#include <mutex>

#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/include/source_operator.h"
#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace processor {

struct Mask {
public:
    Mask(uint64_t size, uint8_t maskedFlag) : maskedFlag{maskedFlag} {
        data = make_unique<uint8_t[]>(size);
        fill(data.get(), data.get() + size, 0);
    }

    // Notice: This function is not protected with a lock for concurrent writes because of the
    // special use case that there is no mixed reads and writes to the mask, and all writes to the
    // mask try to set a position to the same value, thus it doesn't matter which thread succeeds.
    inline void setMask(uint64_t pos, uint8_t maskerIdx, uint8_t maskValue) {
        // Note: blindly update mask does not parallel well, so we minimize write by first checking
        // if the mask is true or not.
        if (data[pos] == maskerIdx) {
            data[pos] = maskValue;
        }
    }
    inline bool isMasked(uint64_t pos) { return data[pos] == maskedFlag; }

private:
    // The value of maskedFlag is equivalent to the num of maskers passed. It is used to check if a
    // value is selected by all maskers or not. Each masker will increment its selected value by 1.
    uint8_t maskedFlag;
    unique_ptr<uint8_t[]> data;
};

struct ScanNodeIDSemiMask {
public:
    ScanNodeIDSemiMask(node_offset_t maxNodeOffset, uint8_t maskedFlag) {
        nodeMask = make_unique<Mask>(maxNodeOffset + 1, maskedFlag);
        morselMask =
            make_unique<Mask>((maxNodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2) + 1, maskedFlag);
    }

    inline bool isNodeMaskEnabled() { return nodeMask != nullptr; }
    inline bool isMorselMasked(uint64_t morselIdx) { return morselMask->isMasked(morselIdx); }
    inline bool isNodeMasked(uint64_t nodeOffset) { return nodeMask->isMasked(nodeOffset); }

    void setMask(uint64_t nodeOffset, uint8_t maskerIdx);

private:
    unique_ptr<Mask> nodeMask;
    unique_ptr<Mask> morselMask;
};

class ScanNodeIDSharedState {

public:
    explicit ScanNodeIDSharedState(
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, table_id_t tableID)
        : initialized{false}, nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs},
          tableID{tableID}, maxNodeOffset{UINT64_MAX}, maxMorselIdx{UINT64_MAX},
          currentNodeOffset{0}, numMaskers{0}, semiMask{nullptr} {}

    void initialize(Transaction* transaction);

    pair<uint64_t, uint64_t> getNextRangeToRead();

    void initSemiMask(Transaction* transaction);

    inline bool isNodeMaskEnabled() const {
        return semiMask != nullptr && semiMask->isNodeMaskEnabled();
    }
    inline ScanNodeIDSemiMask* getSemiMask() { return semiMask.get(); }
    inline uint8_t getNumMaskers() const { return numMaskers; }
    inline void incrementNumMaskers() { numMaskers++; }

private:
    mutex mtx;
    bool initialized;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    table_id_t tableID;
    uint64_t maxNodeOffset;
    uint64_t maxMorselIdx;
    uint64_t currentNodeOffset;
    uint8_t numMaskers;
    unique_ptr<ScanNodeIDSemiMask> semiMask;
};

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(unique_ptr<ResultSetDescriptor> resultSetDescriptor, string nodeName,
        NodeTable* nodeTable, const DataPos& outDataPos,
        shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{std::move(resultSetDescriptor)},
          nodeName{std::move(nodeName)}, nodeTable{nodeTable}, outDataPos{outDataPos},
          sharedState{std::move(sharedState)} {}

    inline string getNodeName() const { return nodeName; }
    inline ScanNodeIDSharedState* getSharedState() const { return sharedState.get(); }

    inline PhysicalOperatorType getOperatorType() override { return SCAN_NODE_ID; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID>(resultSetDescriptor->copy(), nodeName, nodeTable, outDataPos,
            sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    void setSelVector(node_offset_t startOffset, node_offset_t endOffset);

private:
    string nodeName;
    NodeTable* nodeTable;
    DataPos outDataPos;
    shared_ptr<ScanNodeIDSharedState> sharedState;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
