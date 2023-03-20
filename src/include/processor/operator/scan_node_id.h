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
struct NodeTableSemiMask {
public:
    NodeTableSemiMask() : numMaskers{0} {}

    inline void initializeMaskData(common::offset_t maxNodeOffset, common::offset_t maxMorselIdx) {
        if (nodeMask != nullptr) {
            // Multiple semi mask might be applied to the same sacn and thus initialize repeatedly.
            return;
        }
        assert(morselMask == nullptr && maxNodeOffset != common::INVALID_NODE_OFFSET);
        nodeMask = std::make_unique<Mask>(maxNodeOffset + 1);
        morselMask = std::make_unique<Mask>(maxMorselIdx + 1);
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
class NodeTableState {
public:
    explicit NodeTableState(storage::NodeTable* table)
        : table{table}, maxNodeOffset{common::INVALID_NODE_OFFSET}, maxMorselIdx{UINT64_MAX},
          currentNodeOffset{0} {
        semiMask = std::make_unique<NodeTableSemiMask>();
    }

    inline storage::NodeTable* getTable() { return table; }

    inline void initializeMaxOffset(transaction::Transaction* transaction) {
        if (maxNodeOffset != common::INVALID_NODE_OFFSET) {
            // We might initialize twice because semi mask (which is on a different pipeline that
            // execute beforehand) will also try to initialize.
            return;
        }
        maxNodeOffset = table->getMaxNodeOffset(transaction);
        maxMorselIdx = maxNodeOffset >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }

    inline void initSemiMask(transaction::Transaction* transaction) {
        initializeMaxOffset(transaction);
        semiMask->initializeMaskData(maxNodeOffset, maxMorselIdx);
    }
    inline bool isSemiMaskEnabled() { return semiMask->getNumMaskers() > 0; }
    inline NodeTableSemiMask* getSemiMask() { return semiMask.get(); }
    inline uint8_t getNumMaskers() const { return semiMask->getNumMaskers(); }
    inline void incrementNumMaskers() { semiMask->incrementNumMaskers(); }

    std::pair<common::offset_t, common::offset_t> getNextRangeToRead();

private:
    storage::NodeTable* table;
    uint64_t maxNodeOffset;
    uint64_t maxMorselIdx;
    uint64_t currentNodeOffset;
    std::unique_ptr<NodeTableSemiMask> semiMask;
};

class ScanNodeIDSharedState {
public:
    ScanNodeIDSharedState() : currentStateIdx{0} {};

    inline void addTableState(storage::NodeTable* table) {
        tableStates.push_back(std::make_unique<NodeTableState>(table));
    }
    inline uint32_t getNumTableStates() const { return tableStates.size(); }
    inline NodeTableState* getTableState(uint32_t idx) const { return tableStates[idx].get(); }

    inline void initialize(transaction::Transaction* transaction) {
        for (auto& tableState : tableStates) {
            tableState->initializeMaxOffset(transaction);
        }
    }

    std::tuple<NodeTableState*, common::offset_t, common::offset_t> getNextRangeToRead();

private:
    std::mutex mtx;
    std::vector<std::unique_ptr<NodeTableState>> tableStates;
    uint32_t currentStateIdx;
};

class ScanNodeID : public PhysicalOperator {
public:
    ScanNodeID(const DataPos& outDataPos, std::shared_ptr<ScanNodeIDSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString},
          outDataPos{outDataPos}, sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    inline ScanNodeIDSharedState* getSharedState() const { return sharedState.get(); }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanNodeID>(outDataPos, sharedState, id, paramsString);
    }

private:
    inline void initGlobalStateInternal(ExecutionContext* context) override {
        sharedState->initialize(context->transaction);
    }

    void setSelVector(
        NodeTableState* tableState, common::offset_t startOffset, common::offset_t endOffset);

private:
    DataPos outDataPos;
    std::shared_ptr<ScanNodeIDSharedState> sharedState;
    std::shared_ptr<common::ValueVector> outValueVector;
};

} // namespace processor
} // namespace kuzu
