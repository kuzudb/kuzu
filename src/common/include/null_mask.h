#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "src/common/include/configs.h"

namespace graphflow {
namespace common {

constexpr uint64_t NULL_BITMASKS_WITH_SINGLE_ONE[64] = {0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80,
    0x100, 0x200, 0x400, 0x800, 0x1000, 0x2000, 0x4000, 0x8000, 0x10000, 0x20000, 0x40000, 0x80000,
    0x100000, 0x200000, 0x400000, 0x800000, 0x1000000, 0x2000000, 0x4000000, 0x8000000, 0x10000000,
    0x20000000, 0x40000000, 0x80000000, 0x100000000, 0x200000000, 0x400000000, 0x800000000,
    0x1000000000, 0x2000000000, 0x4000000000, 0x8000000000, 0x10000000000, 0x20000000000,
    0x40000000000, 0x80000000000, 0x100000000000, 0x200000000000, 0x400000000000, 0x800000000000,
    0x1000000000000, 0x2000000000000, 0x4000000000000, 0x8000000000000, 0x10000000000000,
    0x20000000000000, 0x40000000000000, 0x80000000000000, 0x100000000000000, 0x200000000000000,
    0x400000000000000, 0x800000000000000, 0x1000000000000000, 0x2000000000000000,
    0x4000000000000000, 0x8000000000000000};
constexpr uint64_t NULL_BITMASKS_WITH_SINGLE_ZERO[64] = {0xfffffffffffffffe, 0xfffffffffffffffd,
    0xfffffffffffffffb, 0xfffffffffffffff7, 0xffffffffffffffef, 0xffffffffffffffdf,
    0xffffffffffffffbf, 0xffffffffffffff7f, 0xfffffffffffffeff, 0xfffffffffffffdff,
    0xfffffffffffffbff, 0xfffffffffffff7ff, 0xffffffffffffefff, 0xffffffffffffdfff,
    0xffffffffffffbfff, 0xffffffffffff7fff, 0xfffffffffffeffff, 0xfffffffffffdffff,
    0xfffffffffffbffff, 0xfffffffffff7ffff, 0xffffffffffefffff, 0xffffffffffdfffff,
    0xffffffffffbfffff, 0xffffffffff7fffff, 0xfffffffffeffffff, 0xfffffffffdffffff,
    0xfffffffffbffffff, 0xfffffffff7ffffff, 0xffffffffefffffff, 0xffffffffdfffffff,
    0xffffffffbfffffff, 0xffffffff7fffffff, 0xfffffffeffffffff, 0xfffffffdffffffff,
    0xfffffffbffffffff, 0xfffffff7ffffffff, 0xffffffefffffffff, 0xffffffdfffffffff,
    0xffffffbfffffffff, 0xffffff7fffffffff, 0xfffffeffffffffff, 0xfffffdffffffffff,
    0xfffffbffffffffff, 0xfffff7ffffffffff, 0xffffefffffffffff, 0xffffdfffffffffff,
    0xffffbfffffffffff, 0xffff7fffffffffff, 0xfffeffffffffffff, 0xfffdffffffffffff,
    0xfffbffffffffffff, 0xfff7ffffffffffff, 0xffefffffffffffff, 0xffdfffffffffffff,
    0xffbfffffffffffff, 0xff7fffffffffffff, 0xfeffffffffffffff, 0xfdffffffffffffff,
    0xfbffffffffffffff, 0xf7ffffffffffffff, 0xefffffffffffffff, 0xdfffffffffffffff,
    0xbfffffffffffffff, 0x7fffffffffffffff};

const uint64_t NULL_LOWER_MASKS[65] = {0x0, 0x1, 0x3, 0x7, 0xf, 0x1f, 0x3f, 0x7f, 0xff, 0x1ff,
    0x3ff, 0x7ff, 0xfff, 0x1fff, 0x3fff, 0x7fff, 0xffff, 0x1ffff, 0x3ffff, 0x7ffff, 0xfffff,
    0x1fffff, 0x3fffff, 0x7fffff, 0xffffff, 0x1ffffff, 0x3ffffff, 0x7ffffff, 0xfffffff, 0x1fffffff,
    0x3fffffff, 0x7fffffff, 0xffffffff, 0x1ffffffff, 0x3ffffffff, 0x7ffffffff, 0xfffffffff,
    0x1fffffffff, 0x3fffffffff, 0x7fffffffff, 0xffffffffff, 0x1ffffffffff, 0x3ffffffffff,
    0x7ffffffffff, 0xfffffffffff, 0x1fffffffffff, 0x3fffffffffff, 0x7fffffffffff, 0xffffffffffff,
    0x1ffffffffffff, 0x3ffffffffffff, 0x7ffffffffffff, 0xfffffffffffff, 0x1fffffffffffff,
    0x3fffffffffffff, 0x7fffffffffffff, 0xffffffffffffff, 0x1ffffffffffffff, 0x3ffffffffffffff,
    0x7ffffffffffffff, 0xfffffffffffffff, 0x1fffffffffffffff, 0x3fffffffffffffff,
    0x7fffffffffffffff, 0xffffffffffffffff};
const uint64_t NULL_HIGH_MASKS[65] = {0x0, 0x8000000000000000, 0xc000000000000000,
    0xe000000000000000, 0xf000000000000000, 0xf800000000000000, 0xfc00000000000000,
    0xfe00000000000000, 0xff00000000000000, 0xff80000000000000, 0xffc0000000000000,
    0xffe0000000000000, 0xfff0000000000000, 0xfff8000000000000, 0xfffc000000000000,
    0xfffe000000000000, 0xffff000000000000, 0xffff800000000000, 0xffffc00000000000,
    0xffffe00000000000, 0xfffff00000000000, 0xfffff80000000000, 0xfffffc0000000000,
    0xfffffe0000000000, 0xffffff0000000000, 0xffffff8000000000, 0xffffffc000000000,
    0xffffffe000000000, 0xfffffff000000000, 0xfffffff800000000, 0xfffffffc00000000,
    0xfffffffe00000000, 0xffffffff00000000, 0xffffffff80000000, 0xffffffffc0000000,
    0xffffffffe0000000, 0xfffffffff0000000, 0xfffffffff8000000, 0xfffffffffc000000,
    0xfffffffffe000000, 0xffffffffff000000, 0xffffffffff800000, 0xffffffffffc00000,
    0xffffffffffe00000, 0xfffffffffff00000, 0xfffffffffff80000, 0xfffffffffffc0000,
    0xfffffffffffe0000, 0xffffffffffff0000, 0xffffffffffff8000, 0xffffffffffffc000,
    0xffffffffffffe000, 0xfffffffffffff000, 0xfffffffffffff800, 0xfffffffffffffc00,
    0xfffffffffffffe00, 0xffffffffffffff00, 0xffffffffffffff80, 0xffffffffffffffc0,
    0xffffffffffffffe0, 0xfffffffffffffff0, 0xfffffffffffffff8, 0xfffffffffffffffc,
    0xfffffffffffffffe, 0xffffffffffffffff};

class NullMask {

public:
    static constexpr uint64_t NO_NULL_ENTRY = 0;
    static constexpr uint64_t ALL_NULL_ENTRY = ~uint64_t(NO_NULL_ENTRY);
    static constexpr uint64_t NUM_BITS_PER_NULL_ENTRY_LOG2 = 6;
    static constexpr uint64_t NUM_BITS_PER_NULL_ENTRY = (uint64_t)1 << NUM_BITS_PER_NULL_ENTRY_LOG2;
    static constexpr uint64_t NUM_NULL_ENTRIES =
        DEFAULT_VECTOR_CAPACITY >> NUM_BITS_PER_NULL_ENTRY_LOG2;

    NullMask() : mayContainNulls{false} {
        buffer = std::make_unique<uint64_t[]>(NUM_NULL_ENTRIES);
        data = buffer.get();
        std::fill(data, data + NUM_NULL_ENTRIES, NO_NULL_ENTRY);
    }

    inline void setAllNonNull() {
        std::fill(data, data + NUM_NULL_ENTRIES, NO_NULL_ENTRY);
        mayContainNulls = false;
    }
    inline void setAllNull() {
        std::fill(data, data + NUM_NULL_ENTRIES, ALL_NULL_ENTRY);
        mayContainNulls = true;
    }

    inline void setMayContainNulls() { mayContainNulls = true; }
    inline bool hasNoNullsGuarantee() const { return !mayContainNulls; }

    void setNull(uint32_t pos, bool isNull);

    static bool isNull(const uint64_t* nullEntries, uint32_t pos) {
        auto entryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
        auto bitPosInEntry = pos - (entryPos << NUM_BITS_PER_NULL_ENTRY_LOG2);
        return nullEntries[entryPos] & NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
    }

    inline bool isNull(uint32_t pos) const { return isNull(data, pos); }

    inline uint64_t* getData() { return data; }

    static inline std::pair<uint64_t, uint64_t> getNullBitAndEntryPos(uint64_t pos) {
        auto nullEntryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
        return std::make_pair(
            pos - (nullEntryPos << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2), nullEntryPos);
    }

    // This function returns true if we have copied a nullBit with value 1 (indicate a null
    // value) to dstNullEntries.
    static bool copyNullMask(uint64_t srcNullBitPos, uint64_t srcNullEntryPos,
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
                numBitsToReadInCurrentEntry = std::min(
                    NullMask::NUM_BITS_PER_NULL_ENTRY - srcNullBitPos, numBitsToCopy - bitPos);
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
                numBitsToReadInCurrentEntry = std::min(
                    NullMask::NUM_BITS_PER_NULL_ENTRY - dstNullBitPos, numBitsToCopy - bitPos);
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
                numBitsToReadInCurrentEntry = std::min(
                    NullMask::NUM_BITS_PER_NULL_ENTRY - dstNullBitPos, numBitsToCopy - bitPos);
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

private:
    uint64_t* data;
    std::unique_ptr<uint64_t[]> buffer;
    bool mayContainNulls;
};

} // namespace common
} // namespace graphflow
