#pragma once

#include <mutex>

#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// Note: Classes in this file are NOT thread-safe.
struct MaskUtil {
    static inline common::offset_t getMorselIdx(common::offset_t offset) {
        return offset >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
    }
};

struct MaskData {
    uint8_t* data;

    explicit MaskData(uint64_t size) {
        dataBuffer = std::make_unique<uint8_t[]>(size);
        data = dataBuffer.get();
        std::fill(data, data + size, 0);
    }

    inline void setMask(uint64_t pos, uint8_t maskValue) const { data[pos] = maskValue; }
    inline bool isMasked(uint64_t pos, uint8_t trueMaskVal) const {
        return data[pos] == trueMaskVal;
    }

private:
    std::unique_ptr<uint8_t[]> dataBuffer;
};

// MaskCollection represents multiple mask on the same domain with AND semantic.
class MaskCollection {
public:
    MaskCollection() : numMasks{0} {}

    inline void init(common::offset_t maxOffset) {
        std::unique_lock lck{mtx};
        if (maskData != nullptr) { // MaskCollection might be initialized repeatedly.
            return;
        }
        maskData = std::make_unique<MaskData>(maxOffset + 1);
    }

    inline bool isMasked(common::offset_t offset) { return maskData->isMasked(offset, numMasks); }
    // Increment mask value for the given nodeOffset if its current mask value is equal to
    // the specified `currentMaskValue`.
    inline void incrementMaskValue(common::offset_t offset, uint8_t currentMaskValue) {
        if (maskData->isMasked(offset, currentMaskValue)) {
            maskData->setMask(offset, currentMaskValue + 1);
        }
    }

    inline uint8_t getNumMasks() const { return numMasks; }
    inline void incrementNumMasks() { numMasks++; }

private:
    std::mutex mtx;
    std::unique_ptr<MaskData> maskData;
    uint8_t numMasks;
};

class NodeSemiMask {
public:
    explicit NodeSemiMask(storage::NodeTable* nodeTable) : nodeTable{nodeTable} {}

    virtual void init(transaction::Transaction* trx) = 0;

    virtual void incrementMaskValue(common::offset_t nodeOffset, uint8_t currentMaskValue) = 0;

    virtual uint8_t getNumMasks() const = 0;
    virtual void incrementNumMasks() = 0;

    inline bool isEnabled() const { return getNumMasks() > 0; }
    inline storage::NodeTable* getNodeTable() const { return nodeTable; }

protected:
    storage::NodeTable* nodeTable;
};

class NodeOffsetSemiMask : public NodeSemiMask {
public:
    explicit NodeOffsetSemiMask(storage::NodeTable* nodeTable) : NodeSemiMask{nodeTable} {
        offsetMask = std::make_unique<MaskCollection>();
    }

    inline void init(transaction::Transaction* trx) override {
        auto maxNodeOffset = nodeTable->getMaxNodeOffset(trx);
        if (maxNodeOffset == common::INVALID_OFFSET) {
            return;
        }
        offsetMask->init(nodeTable->getMaxNodeOffset(trx) + 1);
    }

    inline void incrementMaskValue(common::offset_t nodeOffset, uint8_t currentMaskValue) override {
        offsetMask->incrementMaskValue(nodeOffset, currentMaskValue);
    }

    inline uint8_t getNumMasks() const override { return offsetMask->getNumMasks(); }
    inline void incrementNumMasks() override { offsetMask->incrementNumMasks(); }

    inline bool isNodeMasked(common::offset_t nodeOffset) {
        return offsetMask->isMasked(nodeOffset);
    }

private:
    std::unique_ptr<MaskCollection> offsetMask;
};

class NodeOffsetAndMorselSemiMask : public NodeSemiMask {
public:
    explicit NodeOffsetAndMorselSemiMask(storage::NodeTable* nodeTable) : NodeSemiMask{nodeTable} {
        offsetMask = std::make_unique<MaskCollection>();
        morselMask = std::make_unique<MaskCollection>();
    }

    inline void init(transaction::Transaction* trx) override {
        auto maxNodeOffset = nodeTable->getMaxNodeOffset(trx);
        if (maxNodeOffset == common::INVALID_OFFSET) {
            return;
        }
        offsetMask->init(maxNodeOffset + 1);
        morselMask->init(MaskUtil::getMorselIdx(maxNodeOffset) + 1);
    }

    // Note: blindly update mask does not parallelize well, so we minimize write by first checking
    // if the mask is set to true (mask value is equal to the expected currentMaskValue) or not.
    inline void incrementMaskValue(uint64_t nodeOffset, uint8_t currentMaskValue) override {
        offsetMask->incrementMaskValue(nodeOffset, currentMaskValue);
        morselMask->incrementMaskValue(MaskUtil::getMorselIdx(nodeOffset), currentMaskValue);
    }

    inline uint8_t getNumMasks() const override { return offsetMask->getNumMasks(); }
    inline void incrementNumMasks() override {
        offsetMask->incrementNumMasks();
        morselMask->incrementNumMasks();
    }

    inline bool isMorselMasked(common::offset_t morselIdx) {
        return morselMask->isMasked(morselIdx);
    }
    inline bool isNodeMasked(common::offset_t nodeOffset) {
        return offsetMask->isMasked(nodeOffset);
    }

private:
    std::unique_ptr<MaskCollection> offsetMask;
    std::unique_ptr<MaskCollection> morselMask;
};

} // namespace processor
} // namespace kuzu
