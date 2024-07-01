#include "common/null_mask.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "common/assert.h"

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
    // If both offsets are aligned relative to each other then copy up to the first byte using the
    // non-aligned method, then copy aligned, then copy the end unaligned again.
    if (!invert && (srcOffset % 8 == dstOffset % 8) && numBitsToCopy >= 8 &&
        numBitsToCopy - (srcOffset % 8) >= 8) {
        auto numBitsInFirstByte = 8 - (srcOffset % 8);
        bool hasNullInSrcNullMask = false;
        if (copyUnaligned(srcNullEntries, srcOffset, dstNullEntries, dstOffset, numBitsInFirstByte,
                false)) {
            hasNullInSrcNullMask = true;
        }
        auto* src =
            reinterpret_cast<const uint8_t*>(srcNullEntries) + (srcOffset + numBitsInFirstByte) / 8;
        auto* dst =
            reinterpret_cast<uint8_t*>(dstNullEntries) + (dstOffset + numBitsInFirstByte) / 8;
        auto numBytesForAlignedCopy = (numBitsToCopy - numBitsInFirstByte) / 8;
        memcpy(dst, src, numBytesForAlignedCopy);
        if (std::any_of(src, src + numBytesForAlignedCopy, [&](uint8_t val) { return val != 0; })) {
            hasNullInSrcNullMask = true;
        }
        auto lastByteStart = numBitsInFirstByte + numBytesForAlignedCopy * 8;
        auto numBitsInLastByte = numBitsToCopy - numBitsInFirstByte - numBytesForAlignedCopy * 8;
        return copyUnaligned(srcNullEntries, srcOffset + lastByteStart, dstNullEntries,
                   dstOffset + lastByteStart, numBitsInLastByte, false) ||
               hasNullInSrcNullMask;
    } else {
        return copyUnaligned(srcNullEntries, srcOffset, dstNullEntries, dstOffset, numBitsToCopy,
            invert);
    }
}
bool NullMask::copyUnaligned(const uint64_t* srcNullEntries, uint64_t srcOffset,
    uint64_t* dstNullEntries, uint64_t dstOffset, uint64_t numBitsToCopy, bool invert) {
    uint64_t bitPos = 0;
    bool hasNullInSrcNullMask = false;
    auto [srcNullEntryPos, srcNullBitPos] = getNullEntryAndBitPos(srcOffset + bitPos);
    auto [dstNullEntryPos, dstNullBitPos] = getNullEntryAndBitPos(dstOffset + bitPos);
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
    auto numNullEntries = (capacity + NUM_BITS_PER_NULL_ENTRY - 1) / NUM_BITS_PER_NULL_ENTRY;
    auto resizedBuffer = std::make_unique<uint64_t[]>(numNullEntries);
    memcpy(resizedBuffer.get(), data.data(), data.size_bytes());
    buffer = std::move(resizedBuffer);
    data = std::span(buffer.get(), numNullEntries);
}

bool NullMask::copyFromNullBits(const uint64_t* srcNullEntries, uint64_t srcOffset,
    uint64_t dstOffset, uint64_t numBitsToCopy, bool invert) {
    if (copyNullMask(srcNullEntries, srcOffset, this->data.data(), dstOffset, numBitsToCopy,
            invert)) {
        this->mayContainNulls = true;
        return true;
    }
    return false;
}

void NullMask::setNullFromRange(uint64_t offset, uint64_t numBitsToSet, bool isNull) {
    if (isNull) {
        this->mayContainNulls = true;
    }
    setNullRange(data.data(), offset, numBitsToSet, isNull);
}

void NullMask::setNullRange(uint64_t* nullEntries, uint64_t offset, uint64_t numBitsToSet,
    bool isNull) {
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
            nullEntries[firstEntryPos] |= ~NULL_LOWER_MASKS[firstBitPos];
            if (lastBitPos > 0) {
                // Set bits before the last bit pos to true
                nullEntries[lastEntryPos] |= NULL_LOWER_MASKS[lastBitPos];
            }
        } else {
            // Set bits including and after the first bit pos to false
            nullEntries[firstEntryPos] &= NULL_LOWER_MASKS[firstBitPos];
            if (lastBitPos > 0) {
                // Set bits before the last bit pos to false
                nullEntries[lastEntryPos] &= ~NULL_LOWER_MASKS[lastBitPos];
            }
        }
    }
}

void NullMask::operator|=(const NullMask& other) {
    KU_ASSERT(other.data.size() == data.size());
    for (size_t i = 0; i < data.size(); i++) {
        data[i] |= other.getData()[i];
    }
}

std::pair<bool, bool> NullMask::getMinMax(const uint64_t* nullEntries, uint64_t numValues) {
    bool min, max;
    auto firstWord = *nullEntries;
    if (numValues >= 64) {
        if (firstWord == 0) {
            min = false;
            max = false;
        } else if (firstWord == ~static_cast<uint64_t>(0u)) {
            min = true;
            max = true;
        } else {
            return std::make_pair(false, true);
        }
    } else {
        // First word will be handled in loop below
        min = max = NullMask::isNull(nullEntries, 0);
    }
    for (size_t i = 0; i < numValues / 64; i++) {
        if (nullEntries[i] != firstWord) {
            return std::make_pair(false, true);
        }
    }
    for (size_t i = numValues / 64 * 64; i < numValues; i++) {
        if (min != NullMask::isNull(nullEntries, i)) {
            return std::make_pair(false, true);
        }
    }
    return std::make_pair(min, max);
}

} // namespace common
} // namespace kuzu
