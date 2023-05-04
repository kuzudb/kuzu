#include "common/null_mask.h"

#include <cstring>

namespace kuzu {
namespace common {

void NullMask::setNull(uint32_t pos, bool isNull) {
    auto entryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto bitPosInEntry = pos - (entryPos << NUM_BITS_PER_NULL_ENTRY_LOG2);
    if (isNull) {
        data[entryPos] |= NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
        mayContainNulls = true;
    } else {
        data[entryPos] &= NULL_BITMASKS_WITH_SINGLE_ZERO[bitPosInEntry];
    }
}

bool NullMask::copyNullMask(const uint64_t* srcNullEntries, uint64_t srcOffset,
    uint64_t* dstNullEntries, uint64_t dstOffset, uint64_t numBitsToCopy) {
    auto [srcNullEntryPos, srcNullBitPos] = getNullEntryAndBitPos(srcOffset);
    auto [dstNullEntryPos, dstNullBitPos] = getNullEntryAndBitPos(dstOffset);
    uint64_t bitPos = 0;
    bool hasNullInSrcNullMask = false;
    while (bitPos < numBitsToCopy) {
        auto curDstNullEntryPos = dstNullEntryPos;
        auto curDstNullBitPos = dstNullBitPos;
        uint64_t numBitsToReadInCurrentEntry;
        uint64_t srcNullMaskEntry = srcNullEntries[srcNullEntryPos];
        if (dstNullBitPos < srcNullBitPos) {
            numBitsToReadInCurrentEntry =
                std::min(NullMask::NUM_BITS_PER_NULL_ENTRY - srcNullBitPos, numBitsToCopy - bitPos);
            // Mask higher bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                                 (srcNullBitPos + numBitsToReadInCurrentEntry)];
            // Shift right to align the bit in the src and dst entry.
            srcNullMaskEntry = srcNullMaskEntry >> (srcNullBitPos - dstNullBitPos);
            // Mask lower bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_LOWER_MASKS[dstNullBitPos];
            // Move to the next null entry in src null mask.
            srcNullEntryPos++;
            srcNullBitPos = 0;
            dstNullBitPos += numBitsToReadInCurrentEntry;
        } else if (dstNullBitPos > srcNullBitPos) {
            numBitsToReadInCurrentEntry =
                std::min(NullMask::NUM_BITS_PER_NULL_ENTRY - dstNullBitPos, numBitsToCopy - bitPos);
            // Mask lower bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_LOWER_MASKS[srcNullBitPos];
            // Shift left to align the bit in the src and dst entry.
            srcNullMaskEntry = srcNullMaskEntry << (dstNullBitPos - srcNullBitPos);
            // Mask higher bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                                 (dstNullBitPos + numBitsToReadInCurrentEntry)];
            // Move to the next null entry in dst null mask.
            dstNullEntryPos++;
            dstNullBitPos = 0;
            srcNullBitPos += numBitsToReadInCurrentEntry;
        } else {
            numBitsToReadInCurrentEntry =
                std::min(NullMask::NUM_BITS_PER_NULL_ENTRY - dstNullBitPos, numBitsToCopy - bitPos);
            // Mask lower bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_LOWER_MASKS[srcNullBitPos];
            // Mask higher bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                                 (dstNullBitPos + numBitsToReadInCurrentEntry)];
            // The input entry and the result entry are already aligned.
            srcNullEntryPos++;
            dstNullEntryPos++;
            srcNullBitPos = dstNullBitPos = 0;
        }
        bitPos += numBitsToReadInCurrentEntry;
        dstNullEntries[curDstNullEntryPos] &=
            ~(NULL_LOWER_MASKS[numBitsToReadInCurrentEntry] << curDstNullBitPos);
        if (srcNullMaskEntry != 0) {
            dstNullEntries[curDstNullEntryPos] |= srcNullMaskEntry;
            hasNullInSrcNullMask = true;
        }
    }
    return hasNullInSrcNullMask;
}

void NullMask::resize(uint64_t capacity) {
    auto resizedBuffer = std::make_unique<uint64_t[]>(capacity);
    memcpy(resizedBuffer.get(), buffer.get(), numNullEntries);
    buffer = std::move(resizedBuffer);
    data = buffer.get();
    numNullEntries = capacity;
}

} // namespace common
} // namespace kuzu
