#pragma once

#include <mutex>

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

/// NOT Thread safe
struct Mask {
public:
    explicit Mask(uint64_t size) {
        boolMask = std::make_unique<bool[]>(size);
        std::fill(boolMask.get(), boolMask.get() + size, false);
    }

    /**
     * sets the position (pos) in the Mask (`boolMask` array) to TRUE
     * @param pos - position in `boolMask`
     */
    inline void setMask(uint64_t pos) {
        if (!boolMask[pos]) {
            boolMask[pos] = true;
        }
    }

    /**
     * returns TRUE if position (pos) in the Mask is set to TRUE
     * @param pos - position in `boolMask`
     * @return - true / false
     */
    inline bool isMasked(uint64_t pos) { return boolMask[pos]; }

private:
    std::unique_ptr<bool[]> boolMask;
};

struct ScanNodeIDSemiMask {
public:
    ScanNodeIDSemiMask(common::offset_t maxNodeOffset) {
        nodeMask = std::make_unique<Mask>(maxNodeOffset + 1);
        morselMask =
            std::make_unique<Mask>((maxNodeOffset >> common::DEFAULT_VECTOR_CAPACITY_LOG_2) + 1);
    }

    inline bool isNodeMaskEnabled() { return nodeMask != nullptr; }
    inline bool isMorselMasked(uint64_t morselIdx) { return morselMask->isMasked(morselIdx); }
    inline bool isNodeMasked(uint64_t nodeOffset) { return nodeMask->isMasked(nodeOffset); }

    void setMask(uint64_t nodeOffset);

private:
    std::unique_ptr<Mask> nodeMask;
    std::unique_ptr<Mask> morselMask;
};

class ScanTableNodeIDSharedState {
public:
    explicit ScanTableNodeIDSharedState(storage::NodeTable* table)
        : table{table}, maxNodeOffset{UINT64_MAX}, maxMorselIdx{UINT64_MAX}, currentNodeOffset{0},
          numMaskers{0}, semiMask{nullptr} {}

    inline storage::NodeTable* getTable() { return table; }

    inline void initialize(transaction::Transaction* transaction) {
        assert(maxNodeOffset == UINT64_MAX && maxMorselIdx == UINT64_MAX);
        maxNodeOffset = table->getMaxNodeOffset(transaction);
        maxMorselIdx = maxNodeOffset >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }

    inline void initSemiMask(transaction::Transaction* transaction) {
        if (semiMask == nullptr) {
            semiMask = std::make_unique<ScanNodeIDSemiMask>(table->getMaxNodeOffset(transaction));
        }
    }
    inline bool isSemiMaskEnabled() { return semiMask != nullptr && semiMask->isNodeMaskEnabled(); }
    inline ScanNodeIDSemiMask* getSemiMask() { return semiMask.get(); }
    inline uint8_t getNumMaskers() const { return numMaskers; }
    inline void incrementNumMaskers() { numMaskers++; }

    std::pair<common::offset_t, common::offset_t> getNextRangeToRead();

private:
    storage::NodeTable* table;
    uint64_t maxNodeOffset;
    uint64_t maxMorselIdx;
    uint64_t currentNodeOffset;
    uint8_t numMaskers;
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
            tableState->initialize(transaction);
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
    void initGlobalStateInternal(ExecutionContext* context) override;

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
