#include <vector>

#include "common/arrow/arrow.h"
#include "common/arrow/arrow_nullmask_tree.h"

namespace kuzu {
namespace common {

// scans are based on data specification found here
// https://arrow.apache.org/docs/format/Columnar.html

// all offsets are measured by value, not physical size

void ArrowNullMaskTree::copyToValueVector(ValueVector* vec, uint64_t dstOffset, uint64_t count) {
    vec->setNullFromBits(mask->getData(), offset, dstOffset, count);
}

ArrowNullMaskTree ArrowNullMaskTree::operator+(int64_t offset) {
    // this operation is mostly a special case for dictionary/run-end encoding
    ArrowNullMaskTree ret(*this);
    ret.offset += offset;
    return ret;
}

bool ArrowNullMaskTree::copyFromBuffer(const void* buffer, uint64_t srcOffset, uint64_t count) {
    if (buffer == nullptr) {
        mask->setAllNonNull();
        return false;
    }
    mask->copyFromNullBits((const uint64_t*)buffer, srcOffset, 0, count, true);
    return true;
}

bool ArrowNullMaskTree::applyParentBitmap(const NullMask* parent, uint64_t count) {
    if (parent == nullptr) {
        return false;
    }
    const uint64_t* buffer = parent->data;
    if (buffer != nullptr) {
        for (uint64_t i = 0; i < (count >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2); i++) {
            mask->buffer[i] |= buffer[i];
        }
        return true;
    }
    return false;
}

template<typename offsetsT>
void ArrowNullMaskTree::scanListPushDown(const ArrowSchema* schema, const ArrowArray* array,
    uint64_t srcOffset, uint64_t count) {
    const offsetsT* offsets = ((const offsetsT*)array->buffers[1]) + srcOffset;
    offsetsT auxiliaryLength = offsets[count] - offsets[0];
    NullMask pushDownMask((auxiliaryLength + NullMask::NUM_BITS_PER_NULL_ENTRY - 1) >>
                          NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
    for (uint64_t i = 0; i < count; i++) {
        pushDownMask.setNullFromRange(offsets[i] - offsets[0], offsets[i + 1] - offsets[i],
            isNull(i));
    }
    children->push_back(ArrowNullMaskTree(schema->children[0], array->children[0], offsets[0],
        auxiliaryLength, &pushDownMask));
}

void ArrowNullMaskTree::scanStructPushDown(const ArrowSchema* schema, const ArrowArray* array,
    uint64_t srcOffset, uint64_t count) {
    for (int64_t i = 0; i < array->n_children; i++) {
        children->push_back(ArrowNullMaskTree(schema->children[i], array->children[i], srcOffset,
            count, mask.get()));
    }
}

ArrowNullMaskTree::ArrowNullMaskTree(const ArrowSchema* schema, const ArrowArray* array,
    uint64_t srcOffset, uint64_t count, const NullMask* parentBitmap)
    : offset{0},
      mask{std::make_shared<common::NullMask>((count + NullMask::NUM_BITS_PER_NULL_ENTRY - 1) >>
                                              NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2)},
      children(std::make_shared<std::vector<ArrowNullMaskTree>>()) {
    if (schema->dictionary != nullptr) {
        copyFromBuffer(array->buffers[0], srcOffset, count);
        applyParentBitmap(parentBitmap, count);
        dictionary = std::make_shared<ArrowNullMaskTree>(schema->dictionary, array->dictionary,
            array->dictionary->offset, array->dictionary->length);
        return;
    }
    const char* arrowType = schema->format;
    std::vector<common::StructField> structFields;
    switch (arrowType[0]) {
    case 'n':
        mask->setAllNull();
        break;
    case 'b':
    case 'c':
    case 'C':
    case 's':
    case 'S':
    case 'i':
    case 'I':
    case 'l':
    case 'L':
    case 'f':
    case 'g':
        copyFromBuffer(array->buffers[0], srcOffset, count);
        break;
    case 'z':
    case 'Z':
    case 'u':
    case 'U':
    case 'v':
    case 'w':
    case 't':
        copyFromBuffer(array->buffers[0], srcOffset, count);
        applyParentBitmap(parentBitmap, count);
        break;
    case '+':
        switch (arrowType[1]) {
        case 'l':
            copyFromBuffer(array->buffers[0], srcOffset, count);
            applyParentBitmap(parentBitmap, count);
            scanListPushDown<int32_t>(schema, array, srcOffset, count);
            break;
        case 'L':
            copyFromBuffer(array->buffers[0], srcOffset, count);
            applyParentBitmap(parentBitmap, count);
            scanListPushDown<int64_t>(schema, array, srcOffset, count);
            break;
        case 'w':
            // TODO manh: array null resolution
            KU_UNREACHABLE;
            copyFromBuffer(array->buffers[0], srcOffset, count);
            applyParentBitmap(parentBitmap, count);
            break;
        case 's':
            copyFromBuffer(array->buffers[0], srcOffset, count);
            applyParentBitmap(parentBitmap, count);
            scanStructPushDown(schema, array, srcOffset, count);
            break;
        case 'm':
            copyFromBuffer(array->buffers[0], srcOffset, count);
            applyParentBitmap(parentBitmap, count);
            scanListPushDown<int32_t>(schema, array, srcOffset, count);
            break;
        case 'u': {
            const int8_t* types = (const int8_t*)array->buffers[0];
            if (schema->format[2] == 'd') {
                const int32_t* offsets = (const int32_t*)array->buffers[1];
                std::vector<int32_t> countChildren(array->n_children),
                    lowestOffsets(array->n_children);
                std::vector<int32_t> highestOffsets(array->n_children);
                for (auto i = srcOffset; i < srcOffset + count; i++) {
                    int32_t curOffset = offsets[i];
                    int32_t curType = types[i];
                    if (countChildren[curType] == 0) {
                        lowestOffsets[curType] = curOffset;
                    }
                    highestOffsets[curType] = curOffset;
                    countChildren[curType]++;
                }
                for (int64_t i = 0; i < array->n_children; i++) {
                    children->push_back(ArrowNullMaskTree(schema->children[i], array->children[i],
                        lowestOffsets[i], highestOffsets[i] - lowestOffsets[i]));
                }
                for (auto i = 0u; i < count; i++) {
                    int32_t curOffset = offsets[i + srcOffset];
                    int8_t curType = types[i + srcOffset];
                    mask->setNull(i, children->operator[](curType).isNull(curOffset));
                }
            } else {
                for (int64_t i = 0; i < array->n_children; i++) {
                    children->push_back(ArrowNullMaskTree(schema->children[i], array->children[i],
                        srcOffset, count));
                }
                for (auto i = 0u; i < count; i++) {
                    int8_t curType = types[i + srcOffset];
                    mask->setNull(i, children->operator[](curType).isNull(i));
                    // this isn't specified in the arrow specification, but is it valid to
                    // compute this using a bitwise OR?
                }
            }
            if (parentBitmap != nullptr) {
                for (uint64_t i = 0; i < count >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2; i++) {
                    mask->buffer[i] |= parentBitmap->buffer[i];
                }
            }
        } break;
        case 'v':
            // list views *suck*, especially when trying to write code that can support
            // parallelization for this, we generate child NullMaskTrees on the fly, rather than
            // attempt any precomputation
            if (array->buffers[0] == nullptr) {
                mask->setAllNonNull();
            } else {
                mask->copyFromNullBits((const uint64_t*)array->buffers[0], srcOffset, 0, count,
                    true);
            }
            if (parentBitmap != nullptr) {
                for (uint64_t i = 0; i < count >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2; i++) {
                    mask->buffer[i] |= parentBitmap->buffer[i];
                }
            }
            break;
        case 'r':
            // it's better to resolve validity during the actual scanning for run-end encoded arrays
            // so for this, let's just resolve child validities and move on
            for (int64_t i = 0; i < array->n_children; i++) {
                children->push_back(ArrowNullMaskTree(schema->children[i], array->children[i],
                    array->children[i]->offset, array->children[i]->length));
            }
            break;
        default:
            KU_UNREACHABLE;
        }
        break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace common
} // namespace kuzu
