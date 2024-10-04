#pragma once

#include <memory>
#include <mutex>

#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {

// Note: Classes in this file are NOT thread-safe.
struct MaskUtil {
    static common::offset_t getVectorIdx(common::offset_t offset) {
        return offset >> VECTOR_CAPACITY_LOG_2;
    }
};

struct MaskData {
    uint8_t* data;

    explicit MaskData(uint64_t size, uint8_t defaultVal = 0) : size{size} {
        dataBuffer = std::make_unique<uint8_t[]>(size);
        data = dataBuffer.get();
        std::fill(data, data + size, defaultVal);
    }

    inline void setMask(uint64_t pos, uint8_t maskValue) const { data[pos] = maskValue; }
    inline bool isMasked(uint64_t pos, uint8_t trueMaskVal) const {
        return data[pos] == trueMaskVal;
    }
    inline uint8_t getMaskValue(uint64_t pos) const { return data[pos]; }
    inline uint64_t getSize() const { return size; }

private:
    std::unique_ptr<uint8_t[]> dataBuffer;
    uint64_t size;
};

// MaskCollection represents multiple mask on the same domain with AND semantic.
class MaskCollection {
public:
    MaskCollection() : numMasks{0} {}

    void init(common::offset_t maxOffset) {
        std::unique_lock lck{mtx};
        if (maskData != nullptr) {
            // MaskCollection might be initialized repeatedly. Because multiple semiMasker can
            // hold the same mask.
            return;
        }
        maskData = std::make_unique<MaskData>(maxOffset + 1);
    }

    bool isMasked(common::offset_t offset) const { return maskData->isMasked(offset, numMasks); }
    // Return true if any offset between [startOffset, endOffset] is masked. Otherwise return false.
    bool isAnyMasked(common::offset_t startOffset, common::offset_t endOffset) const {
        auto offset = startOffset;
        auto numMasked = 0u;
        while (offset <= endOffset) {
            numMasked += maskData->isMasked(offset++, numMasks);
        }
        return numMasked > 0;
    }
    // Increment mask value for the given nodeOffset if its current mask value is equal to
    // the specified `currentMaskValue`.
    // Note: blindly update mask does not parallelize well, so we minimize write by first checking
    // if the mask is set to true (mask value is equal to the expected currentMaskValue) or not.
    void incrementMaskValue(common::offset_t offset, uint8_t currentMaskValue) {
        if (offset >= maskData->getSize()) [[unlikely]] { // Handle uncommitted node offsets.
            return;
        }
        if (maskData->isMasked(offset, currentMaskValue)) {
            maskData->setMask(offset, currentMaskValue + 1);
        }
    }

    uint8_t getNumMasks() const { return numMasks; }
    void incrementNumMasks() { numMasks++; }

private:
    std::mutex mtx;
    std::unique_ptr<MaskData> maskData;
    uint8_t numMasks;
};

class NodeSemiMask {
public:
    explicit NodeSemiMask(common::table_id_t tableID, common::offset_t maxOffset)
        : tableID{tableID}, maxOffset{maxOffset} {}
    virtual ~NodeSemiMask() = default;

    common::table_id_t getTableID() const { return tableID; }
    common::offset_t getMaxOffset() const { return maxOffset; }

    virtual void incrementMaskValue(common::offset_t nodeOffset, uint8_t currentMaskValue) = 0;
    virtual bool isMasked(common::offset_t offset) = 0;
    virtual bool isAnyMasked(common::offset_t startNodeOffset, common::offset_t endNodeOffset) = 0;

    bool isEnabled() const { return getNumMasks() > 0; }
    uint8_t getNumMasks() const { return maskCollection.getNumMasks(); }
    void incrementNumMasks() { maskCollection.incrementNumMasks(); }

protected:
    common::table_id_t tableID;
    common::offset_t maxOffset;
    MaskCollection maskCollection;
};

class NodeOffsetLevelSemiMask final : public NodeSemiMask {
public:
    explicit NodeOffsetLevelSemiMask(common::table_id_t tableID, common::offset_t maxOffset)
        : NodeSemiMask{tableID, maxOffset} {

        if (maxOffset != common::INVALID_OFFSET) {
            maskCollection.init(maxOffset + 1);
        }
    }

    void incrementMaskValue(common::offset_t nodeOffset, uint8_t currentMaskValue) override {
        maskCollection.incrementMaskValue(nodeOffset, currentMaskValue);
    }

    bool isMasked(common::offset_t offset) override { return maskCollection.isMasked(offset); }
    bool isAnyMasked(common::offset_t startNodeOffset, common::offset_t endNodeOffset) override {
        return maskCollection.isAnyMasked(startNodeOffset, endNodeOffset);
    }
};

class NodeVectorLevelSemiMask final : public NodeSemiMask {
public:
    explicit NodeVectorLevelSemiMask(common::table_id_t tableID, common::offset_t maxOffset)
        : NodeSemiMask{tableID, maxOffset} {
        if (maxOffset != common::INVALID_OFFSET) {
            maskCollection.init(MaskUtil::getVectorIdx(maxOffset) + 1);
        }
    }

    void incrementMaskValue(uint64_t nodeOffset, uint8_t currentMaskValue) override {
        maskCollection.incrementMaskValue(MaskUtil::getVectorIdx(nodeOffset), currentMaskValue);
    }

    bool isMasked(common::offset_t offset) override {
        return maskCollection.isMasked(MaskUtil::getVectorIdx(offset));
    }
    bool isAnyMasked(common::offset_t startNodeOffset, common::offset_t endNodeOffset) override {
        return maskCollection.isAnyMasked(MaskUtil::getVectorIdx(startNodeOffset),
            MaskUtil::getVectorIdx(endNodeOffset));
    }
};

} // namespace common
} // namespace kuzu
