#include "src/common/include/null_mask.h"

namespace graphflow {

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

bool NullMask::copyNullMask(uint64_t srcNullBitPos, uint64_t srcNullEntryPos,
    uint64_t* srcNullEntries, uint64_t dstNullBitPos, uint64_t dstNullEntryPos,
    uint64_t* dstNullEntries, uint64_t numBitsToCopy) {
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
            // Shift right to align the bit in the page entry and vector entry.
            srcNullMaskEntry = srcNullMaskEntry >> (srcNullBitPos - dstNullBitPos);
            // Mask lower bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_LOWER_MASKS[dstNullBitPos];
            // Move to the next null entry in page.
            srcNullEntryPos++;
            srcNullBitPos = 0;
            dstNullBitPos += numBitsToReadInCurrentEntry;
        } else if (dstNullBitPos > srcNullBitPos) {
            numBitsToReadInCurrentEntry =
                std::min(NullMask::NUM_BITS_PER_NULL_ENTRY - dstNullBitPos, numBitsToCopy - bitPos);
            // Mask lower bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_LOWER_MASKS[srcNullBitPos];
            // Shift left to align the bit in the page entry and vector entry.
            srcNullMaskEntry = srcNullMaskEntry << (dstNullBitPos - srcNullBitPos);
            // Mask higher bits out of current read range to 0.
            srcNullMaskEntry &= ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                                 (dstNullBitPos + numBitsToReadInCurrentEntry)];
            // Move to the next null entry in vector.
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
        // Mask all bits to set in vector to 0.
        dstNullEntries[curDstNullEntryPos] &=
            ~(NULL_LOWER_MASKS[numBitsToReadInCurrentEntry] << curDstNullBitPos);
        if (srcNullMaskEntry != 0) {
            dstNullEntries[curDstNullEntryPos] |= srcNullMaskEntry;
            hasNullInSrcNullMask = true;
        }
    }
    return hasNullInSrcNullMask;
}

} // namespace common
} // namespace graphflow
