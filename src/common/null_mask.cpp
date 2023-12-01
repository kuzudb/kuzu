#include "common/null_mask.h"

#include <cstring>

namespace kuzu {
namespace common {

void NullMask::setNull(uint64_t* nullEntries, uint32_t pos, bool isNull) {
    auto entryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto bitPosInEntry = pos - (entryPos << NUM_BITS_PER_NULL_ENTRY_LOG2);
    if (isNull) {
        nullEntries[entryPos] |= NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
    } else {
        nullEntries[entryPos] &= NULL_BITMASKS_WITH_SINGLE_ZERO[bitPosInEntry];
    }
}

bool NullMask::copyNullMask(const uint64_t* srcNullEntries, uint64_t srcOffset,
    uint64_t* dstNullEntries, uint64_t dstOffset, uint64_t numBitsToCopy, bool invert) {
    auto [srcNullEntryPos, srcNullBitPos] = getNullEntryAndBitPos(srcOffset);
    auto [dstNullEntryPos, dstNullBitPos] = getNullEntryAndBitPos(dstOffset);
    uint64_t bitPos = 0;
    bool hasNullInSrcNullMask = false;
    while (bitPos < numBitsToCopy) {
        auto curDstNullEntryPos = dstNullEntryPos;
        auto curDstNullBitPos = dstNullBitPos;
        uint64_t numBitsToReadInCurrentEntry;
        uint64_t srcNullMaskEntry =
            invert ? ~srcNullEntries[srcNullEntryPos] : srcNullEntries[srcNullEntryPos];
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

bool NullMask::copyFromNullBits(const uint64_t* srcNullEntries, uint64_t srcOffset,
    uint64_t dstOffset, uint64_t numBitsToCopy) {
    if (copyNullMask(srcNullEntries, srcOffset, this->data, dstOffset, numBitsToCopy)) {
        this->mayContainNulls = true;
        return true;
    }
    return false;
}

void NullMask::setNullFromRange(uint64_t offset, uint64_t numBitsToSet, bool isNull) {
    if (isNull) {
        this->mayContainNulls = true;
    }
    setNullRange(data, offset, numBitsToSet, isNull);
}

void NullMask::setNullRange(
    uint64_t* nullEntries, uint64_t offset, uint64_t numBitsToSet, bool isNull) {
    auto [firstEntryPos, firstBitPos] = getNullEntryAndBitPos(offset);
    auto [lastEntryPos, lastBitPos] = getNullEntryAndBitPos(offset + numBitsToSet);

    // If the range spans multiple entries, set the entries in the middle to the appropriate value
    // with std::fill
    if (lastEntryPos > firstEntryPos + 1) {
        std::fill(nullEntries + firstEntryPos + 1, nullEntries + lastEntryPos,
            isNull ? ALL_NULL_ENTRY : NO_NULL_ENTRY);
    }

    if (firstEntryPos == lastEntryPos) {
        if (isNull) {
            // Set bits between the first and the last bit pos to true
            nullEntries[firstEntryPos] |= (~NULL_LOWER_MASKS[firstBitPos] &
                                           ~NULL_HIGH_MASKS[NUM_BITS_PER_NULL_ENTRY - lastBitPos]);
        } else {
            // Set bits between the first and the last bit pos to false
            nullEntries[firstEntryPos] &= (NULL_LOWER_MASKS[firstBitPos] |
                                           NULL_HIGH_MASKS[NUM_BITS_PER_NULL_ENTRY - lastBitPos]);
        }
    } else {
        if (isNull) {
            // Set bits including and after the first bit pos to true
            nullEntries[firstEntryPos] |= ~NULL_HIGH_MASKS[firstBitPos];
            if (lastBitPos > 0) {
                // Set bits before the last bit pos to true
                nullEntries[lastEntryPos] |=
                    ~NULL_LOWER_MASKS[NUM_BITS_PER_NULL_ENTRY - lastBitPos];
            }
        } else {
            // Set bits including and after the first bit pos to false
            nullEntries[firstEntryPos] &= NULL_LOWER_MASKS[firstBitPos];
            if (lastBitPos > 0) {
                // Set bits before the last bit pos to false
                nullEntries[lastEntryPos] &= NULL_HIGH_MASKS[NUM_BITS_PER_NULL_ENTRY - lastBitPos];
            }
        }
    }
}

} // namespace common
} // namespace kuzu
