#pragma once

#include <mutex>

#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct Mask {
public:
    Mask(uint64_t size, uint8_t maskedFlag) : maskedFlag{maskedFlag} {
        data = std::make_unique<uint8_t[]>(size);
        std::fill(data.get(), data.get() + size, 0);
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
    std::unique_ptr<uint8_t[]> data;
};

struct ScanNodeIDSemiMask {
public:
    ScanNodeIDSemiMask(common::offset_t maxNodeOffset, uint8_t maskedFlag) {
        nodeMask = std::make_unique<Mask>(maxNodeOffset + 1, maskedFlag);
        morselMask = std::make_unique<Mask>(
            (maxNodeOffset >> common::DEFAULT_VECTOR_CAPACITY_LOG_2) + 1, maskedFlag);
    }

    inline bool isNodeMaskEnabled() { return nodeMask != nullptr; }
    inline bool isMorselMasked(uint64_t morselIdx) { return morselMask->isMasked(morselIdx); }
    inline bool isNodeMasked(uint64_t nodeOffset) { return nodeMask->isMasked(nodeOffset); }

    void setMask(uint64_t nodeOffset, uint8_t maskerIdx);

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
            semiMask = std::make_unique<ScanNodeIDSemiMask>(
                table->getMaxNodeOffset(transaction), numMaskers);
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
