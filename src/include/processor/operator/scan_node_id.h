#pragma once

#include <mutex>

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// Note: This class is not thread-safe.
struct Mask {
public:
    explicit Mask(uint64_t size) {
        data = std::make_unique<uint8_t[]>(size);
        std::fill(data.get(), data.get() + size, 0);
    }

    inline void setMask(uint64_t pos, uint8_t maskValue) { data[pos] = maskValue; }
    inline bool isMasked(uint64_t pos, uint8_t trueMaskVal) { return data[pos] == trueMaskVal; }

private:
    std::unique_ptr<uint8_t[]> data;
};

// Note: This class is not thread-safe.
struct ScanNodeIDSemiMask {
public:
    explicit ScanNodeIDSemiMask() : numMaskers{0} {}

    inline void initializeMaskData(common::offset_t maxNodeOffset, common::offset_t maxMorselIdx) {
        if (nodeMask == nullptr) {
            assert(morselMask == nullptr);
            nodeMask = std::make_unique<Mask>(maxNodeOffset + 1);
            morselMask = std::make_unique<Mask>(maxMorselIdx + 1);
        }
    }

    inline bool isMorselMasked(uint64_t morselIdx) {
        return morselMask->isMasked(morselIdx, numMaskers);
    }
    inline bool isNodeMasked(uint64_t nodeOffset) {
        return nodeMask->isMasked(nodeOffset, numMaskers);
    }

    // Increment mask value for the given nodeOffset if its current mask value is equal to
    // the specified `currentMaskValue`.
    void incrementMaskValue(uint64_t nodeOffset, uint8_t currentMaskValue);

    inline uint8_t getNumMaskers() const { return numMaskers; }
    inline void incrementNumMaskers() { numMaskers++; }

private:
    std::unique_ptr<Mask> nodeMask;
    std::unique_ptr<Mask> morselMask;
    uint8_t numMaskers;
};

// Note: This class is not thread-safe. It relies on its caller to correctly synchronize its state.
class ScanTableNodeIDSharedState {
public:
    explicit ScanTableNodeIDSharedState(storage::NodeTable* table)
        : table{table}, maxNodeOffset{UINT64_MAX}, maxMorselIdx{UINT64_MAX}, currentNodeOffset{0} {
        semiMask = std::make_unique<ScanNodeIDSemiMask>();
    }

    inline storage::NodeTable* getTable() { return table; }

    inline void initializeMaxOffset(transaction::Transaction* transaction) {
        assert(maxNodeOffset == UINT64_MAX && maxMorselIdx == UINT64_MAX);
        maxNodeOffset = table->getMaxNodeOffset(transaction);
        maxMorselIdx = maxNodeOffset >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }

    inline void initSemiMask(transaction::Transaction* transaction) {
        semiMask->initializeMaskData(maxNodeOffset, maxMorselIdx);
    }
    inline bool isSemiMaskEnabled() { return semiMask->getNumMaskers() > 0; }
    inline ScanNodeIDSemiMask* getSemiMask() { return semiMask.get(); }
    inline uint8_t getNumMaskers() const { return semiMask->getNumMaskers(); }
    inline void incrementNumMaskers() { semiMask->incrementNumMaskers(); }

    std::pair<common::offset_t, common::offset_t> getNextRangeToRead();

private:
    storage::NodeTable* table;
    uint64_t maxNodeOffset;
    uint64_t maxMorselIdx;
    uint64_t currentNodeOffset;
    std::unique_ptr<ScanNodeIDSemiMask> semiMask;
};

class ScanNodeIDSharedState {
public:
    ScanNodeIDSharedState() : currentStateIdx{0} {};

    inline void addTableState(storage::NodeTable* table) {
        tableStates.push_back(std::make_unique<ScanTableNodeIDSharedState>(table));
    }
    inline uint32_t getNumTableStates() const { return tableStates.size(); }
    inline ScanTableNodeIDSharedState* getTableState(uint32_t idx) const {
        return tableStates[idx].get();
    }

    inline void initialize(transaction::Transaction* transaction) {
        for (auto& tableState : tableStates) {
            tableState->initializeMaxOffset(transaction);
        }
    }

    std::tuple<ScanTableNodeIDSharedState*, common::offset_t, common::offset_t>
    getNextRangeToRead();

private:
    std::mutex mtx;
    std::vector<std::unique_ptr<ScanTableNodeIDSharedState>> tableStates;
    uint32_t currentStateIdx;
};

class ScanNodeID : public PhysicalOperator {
public:
    ScanNodeID(std::string nodeID, const DataPos& outDataPos,
        std::shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString},
          nodeID{std::move(nodeID)}, outDataPos{outDataPos}, sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    inline std::string getNodeID() const { return nodeID; }
    inline ScanNodeIDSharedState* getSharedState() const { return sharedState.get(); }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanNodeID>(nodeID, outDataPos, sharedState, id, paramsString);
    }

private:
    inline void initGlobalStateInternal(ExecutionContext* context) override {
        sharedState->initialize(context->transaction);
    }

    void setSelVector(ScanTableNodeIDSharedState* tableState, common::offset_t startOffset,
        common::offset_t endOffset);

private:
    std::string nodeID;
    DataPos outDataPos;
    std::shared_ptr<ScanNodeIDSharedState> sharedState;
    std::shared_ptr<common::ValueVector> outValueVector;
};

} // namespace processor
} // namespace kuzu
