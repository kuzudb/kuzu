#include "storage/compression/compression.h"

#include <limits>
#include <string>

#include "common/exception/not_implemented.h"
#include "common/exception/storage.h"
#include "common/null_mask.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "fastpfor/bitpackinghelpers.h"
#include "storage/compression/sign_extend.h"
#include "storage/store/column.h"
#include <bit>

using namespace kuzu::common;

namespace kuzu {
namespace storage {

uint32_t getDataTypeSizeInChunk(const common::LogicalType& dataType) {
    if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        return 0;
    }
    return getDataTypeSizeInChunk(dataType.getPhysicalType());
}

uint32_t getDataTypeSizeInChunk(const common::PhysicalTypeID& dataType) {
    switch (dataType) {
    case PhysicalTypeID::STRUCT: {
        return 0;
    }
    case PhysicalTypeID::STRING: {
        return sizeof(uint32_t);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t);
    }
    default: {
        auto size = PhysicalTypeUtils::getFixedTypeSize(dataType);
        KU_ASSERT(size <= BufferPoolConstants::PAGE_4KB_SIZE);
        return size;
    }
    }
}

bool CompressionMetadata::canAlwaysUpdateInPlace() const {
    switch (compression) {
    case CompressionType::BOOLEAN_BITPACKING:
    case CompressionType::UNCOMPRESSED: {
        return true;
    }
    case CompressionType::CONSTANT:
    case CompressionType::INTEGER_BITPACKING: {
        return false;
    }
    default: {
        throw common::StorageException(
            "Unknown compression type with ID " + std::to_string((uint8_t)compression));
    }
    }
}

bool CompressionMetadata::canUpdateInPlace(const uint8_t* data, uint32_t pos,
    PhysicalTypeID physicalType) const {
    if (canAlwaysUpdateInPlace()) {
        return true;
    }
    switch (compression) {
    case CompressionType::CONSTANT: {
        // Value can be updated in place only if it is identical to the value already stored.
        switch (physicalType) {
        case PhysicalTypeID::BOOL: {
            return NullMask::isNull(reinterpret_cast<const uint64_t*>(data), pos) ==
                   *reinterpret_cast<const bool*>(this->data.data());
        }
        default: {
            auto size = getDataTypeSizeInChunk(physicalType);
            return memcmp(data + pos * size, this->data.data(), size) == 0;
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING:
    case CompressionType::UNCOMPRESSED: {
        return true;
    }
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            auto value = reinterpret_cast<const int64_t*>(data)[pos];
            return IntegerBitpacking<int64_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::INT32: {
            auto value = reinterpret_cast<const int32_t*>(data)[pos];
            return IntegerBitpacking<int32_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::INT16: {
            auto value = reinterpret_cast<const int16_t*>(data)[pos];
            return IntegerBitpacking<int16_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::INT8: {
            auto value = reinterpret_cast<const int8_t*>(data)[pos];
            return IntegerBitpacking<int8_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::INTERNAL_ID:
        case PhysicalTypeID::ARRAY:
        case PhysicalTypeID::LIST:
        case PhysicalTypeID::UINT64: {
            auto value = reinterpret_cast<const uint64_t*>(data)[pos];
            return IntegerBitpacking<uint64_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::STRING:
        case PhysicalTypeID::UINT32: {
            auto value = reinterpret_cast<const uint32_t*>(data)[pos];
            return IntegerBitpacking<uint32_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::UINT16: {
            auto value = reinterpret_cast<const uint16_t*>(data)[pos];
            return IntegerBitpacking<uint16_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        case PhysicalTypeID::UINT8: {
            auto value = reinterpret_cast<const uint8_t*>(data)[pos];
            return IntegerBitpacking<uint8_t>::canUpdateInPlace(value,
                BitpackHeader::readHeader(this->data));
        }
        default: {
            throw common::StorageException(
                "Attempted to read from a column chunk which uses integer bitpacking but does not "
                "have a supported integer physical type: " +
                PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    default: {
        throw common::StorageException(
            "Unknown compression type with ID " + std::to_string((uint8_t)compression));
    }
    }
}

uint64_t CompressionMetadata::numValues(uint64_t pageSize, const LogicalType& dataType) const {
    switch (compression) {
    case CompressionType::CONSTANT: {
        return std::numeric_limits<uint64_t>::max();
    }
    case CompressionType::UNCOMPRESSED: {
        return Uncompressed::numValues(pageSize, dataType);
    }
    case CompressionType::INTEGER_BITPACKING: {
        switch (dataType.getPhysicalType()) {
        case PhysicalTypeID::INT64:
            return IntegerBitpacking<int64_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        case PhysicalTypeID::INT32:
            return IntegerBitpacking<int32_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        case PhysicalTypeID::INT16:
            return IntegerBitpacking<int16_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        case PhysicalTypeID::INT8:
            return IntegerBitpacking<int8_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        case PhysicalTypeID::INTERNAL_ID:
        case PhysicalTypeID::ARRAY:
        case PhysicalTypeID::LIST:
        case PhysicalTypeID::UINT64:
            return IntegerBitpacking<uint64_t>::numValues(pageSize,
                BitpackHeader::readHeader(data));
        case PhysicalTypeID::STRING:
        case PhysicalTypeID::UINT32:
            return IntegerBitpacking<uint32_t>::numValues(pageSize,
                BitpackHeader::readHeader(data));
        case PhysicalTypeID::UINT16:
            return IntegerBitpacking<uint16_t>::numValues(pageSize,
                BitpackHeader::readHeader(data));
        case PhysicalTypeID::UINT8:
            return IntegerBitpacking<uint8_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        default: {
            throw common::StorageException(
                "Attempted to read from a column chunk which uses integer bitpacking but does not "
                "have a supported integer physical type: " +
                PhysicalTypeUtils::physicalTypeToString(dataType.getPhysicalType()));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING: {
        return BooleanBitpacking::numValues(pageSize);
    }
    default: {
        throw common::StorageException(
            "Unknown compression type with ID " + std::to_string((uint8_t)compression));
    }
    }
}

std::optional<CompressionMetadata> ConstantCompression::analyze(const ColumnChunk& chunk) {
    switch (chunk.getDataType().getPhysicalType()) {
    // Only values that can fit in the CompressionMetadata's data field can use constant compression
    case PhysicalTypeID::BOOL: {
        if (chunk.getCapacity() == 0) {
            std::array<uint8_t, CompressionMetadata::DATA_SIZE> value;
            std::fill(value.data(), value.data() + value.size(), 0);
            return std::optional(CompressionMetadata(CompressionType::CONSTANT, value));
        }
        auto firstValue = chunk.getValue<bool>(0);

        // TODO(bmwinger): This could be optimized. We could do bytewise comparison with memcmp,
        // but we need to make sure to stop at the end of the values to avoid false positives
        for (auto i = 1u; i < chunk.getNumValues(); i++) {
            // If any value is different from the first one, we can't use constant compression
            if (firstValue != chunk.getValue<bool>(i)) {
                return std::nullopt;
            }
        }
        std::array<uint8_t, CompressionMetadata::DATA_SIZE> value;
        std::fill(value.data(), value.data() + value.size(), 0);
        if (chunk.getNumValues() > 0) {
            value[0] = firstValue;
        }
        return std::optional(CompressionMetadata(CompressionType::CONSTANT, value));
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::INTERNAL_ID:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT64: {
        uint8_t size = chunk.getNumBytesPerValue();
        KU_ASSERT(size <= CompressionMetadata::DATA_SIZE);
        // If there are no values, or only one value, we will always use constant compression
        // since the loop won't execute
        for (auto i = 1u; i < chunk.getNumValues(); i++) {
            // If any value is different from the first one, we can't use constant compression
            if (std::memcmp(chunk.getData(), chunk.getData() + i * size, size) != 0) {
                return std::nullopt;
            }
        }
        std::array<uint8_t, CompressionMetadata::DATA_SIZE> value;
        std::fill(value.data(), value.data() + value.size(), 0);
        if (chunk.getNumValues() > 0) {
            std::memcpy(value.data(), chunk.getData(), size);
        }
        return std::optional(CompressionMetadata(CompressionType::CONSTANT, value));
    }
    default: {
        return std::optional<CompressionMetadata>();
    }
    }
}

std::string CompressionMetadata::toString() const {
    switch (compression) {
    case CompressionType::UNCOMPRESSED: {
        return "UNCOMPRESSED";
    }
    case CompressionType::INTEGER_BITPACKING: {
        auto header = BitpackHeader::readHeader(data);
        return "INTEGER_BITPACKING[" + std::to_string(header.bitWidth) + "]";
    }
    case CompressionType::BOOLEAN_BITPACKING: {
        return "BOOLEAN_BITPACKING";
    }
    case CompressionType::CONSTANT: {
        return "CONSTANT";
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

void ConstantCompression::decompressFromPage(const uint8_t* /*srcBuffer*/, uint64_t /*srcOffset*/,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& metadata) const {
    for (auto i = 0u; i < numValues; i++) {
        memcpy(dstBuffer + (dstOffset + i) * numBytesPerValue, metadata.data.data(),
            numBytesPerValue);
    }
}

void ConstantCompression::copyFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& metadata) const {
    if (dataType == common::PhysicalTypeID::BOOL) {
        common::NullMask::setNullRange(reinterpret_cast<uint64_t*>(dstBuffer), dstOffset, numValues,
            metadata.data[0]);
    } else {
        decompressFromPage(srcBuffer, srcOffset, dstBuffer, dstOffset, numValues, metadata);
    }
}

template<typename T>
static inline T abs(typename std::enable_if<std::is_unsigned<T>::value, T>::type value) {
    return value;
}

template<typename T>
static inline T abs(typename std::enable_if<std::is_signed<T>::value, T>::type value) {
    return std::abs(value);
}

template<typename T>
BitpackHeader IntegerBitpacking<T>::getBitWidth(const uint8_t* srcBuffer,
    uint64_t numValues) const {
    T max = std::numeric_limits<T>::min(), min = std::numeric_limits<T>::max();
    for (auto i = 0u; i < numValues; i++) {
        T value = ((T*)srcBuffer)[i];
        if (value > max) {
            max = value;
        }
        if (value < min) {
            min = value;
        }
    }
    bool hasNegative;
    uint64_t offset = 0;
    uint8_t bitWidth;
    // Frame of reference encoding is only used when values are either all positive or all negative,
    // and when we will save at least 1 bit per value.
    if (min > 0 && max > 0 && std::bit_width((U)(max - min)) < std::bit_width((U)max)) {
        offset = min;
        bitWidth = static_cast<uint8_t>(std::bit_width((U)(max - min)));
        hasNegative = false;
    } else if (min < 0 && max < 0 && std::bit_width((U)(min - max)) < std::bit_width((U)max)) {
        offset = (U)max;
        bitWidth = static_cast<uint8_t>(std::bit_width((U)(min - max))) + 1;
        // This is somewhat suboptimal since we know that the values are all negative
        // We could use an offset equal to the minimum, but values which are all negative are
        // probably going to grow in the negative direction, leading to many re-compressions when
        // inserting
        hasNegative = true;
    } else if (min < 0) {
        bitWidth = static_cast<uint8_t>(std::bit_width((U)std::max(abs<T>(min), abs<T>(max)))) + 1;
        hasNegative = true;
    } else {
        bitWidth = static_cast<uint8_t>(std::bit_width((U)std::max(abs<T>(min), abs<T>(max))));
        hasNegative = false;
    }
    return BitpackHeader{bitWidth, hasNegative, offset};
}

template<typename T>
bool IntegerBitpacking<T>::canUpdateInPlace(T value, const BitpackHeader& header) {
    T adjustedValue = value - (T)header.offset;
    // If there are negatives, the effective bit width is smaller
    uint8_t valueSize = std::bit_width((U)abs<T>(adjustedValue));
    if (!header.hasNegative && adjustedValue < 0) {
        return false;
    }
    if ((header.hasNegative && valueSize > header.bitWidth - 1) ||
        (!header.hasNegative && valueSize > header.bitWidth)) {
        return false;
    }
    return true;
}

template<typename T>
void fastunpack(const uint8_t* in, T* out, uint32_t bitWidth) {
    if constexpr (std::is_same_v<std::make_signed_t<T>, int32_t> ||
                  std::is_same_v<std::make_signed_t<T>, int64_t>) {
        FastPForLib::fastunpack((const uint32_t*)in, out, bitWidth);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int16_t>) {
        FastPForLib::fastunpack((const uint16_t*)in, out, bitWidth);
    } else {
        static_assert(std::is_same_v<std::make_signed_t<T>, int8_t>);
        FastPForLib::fastunpack((const uint8_t*)in, out, bitWidth);
    }
}

template<typename T>
void fastpack(const T* in, uint8_t* out, uint8_t bitWidth) {
    if constexpr (std::is_same_v<std::make_signed_t<T>, int32_t> ||
                  std::is_same_v<std::make_signed_t<T>, int64_t>) {
        FastPForLib::fastpack(in, (uint32_t*)out, bitWidth);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int16_t>) {
        FastPForLib::fastpack(in, (uint16_t*)out, bitWidth);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int8_t>) {
        FastPForLib::fastpack(in, (uint8_t*)out, bitWidth);
    } else {
        static_assert(std::is_same_v<T, uint16_t> || std::is_same_v<T, int16_t>);
        FastPForLib::fastpack(in, (uint16_t*)out, bitWidth);
    }
}

template<typename T>
void IntegerBitpacking<T>::setValuesFromUncompressed(const uint8_t* srcBuffer, offset_t posInSrc,
    uint8_t* dstBuffer, offset_t posInDst, offset_t numValues, const CompressionMetadata& metadata,
    const NullMask* nullMask) const {
    KU_UNUSED(nullMask);
    auto header = BitpackHeader::readHeader(metadata.data);
    // This is a fairly naive implementation which uses fastunpack/fastpack
    // to modify the data by decompressing/compressing a single chunk of values.
    //
    // TODO(bmwinger): modify the data in-place when numValues is small (frequently called with
    // numValues=1)
    //
    // Data can be considered to be stored in aligned chunks of 32 values
    // with a size of 32 * bitWidth bits,
    // or bitWidth 32-bit values (we cast the buffer to a uint32_t* later).
    auto chunkStart = (uint8_t*)getChunkStart(dstBuffer, posInDst, header.bitWidth);
    auto startPosInChunk = posInDst % CHUNK_SIZE;
    U chunk[CHUNK_SIZE];
    // TODO(bmwinger): Now I'm a bit confused here. Why do we always need to unpack the chunk first?
    //      Can't we just unpack the chunk if we need to modify only partial of it?
    fastunpack(chunkStart, chunk, header.bitWidth);
    for (offset_t i = 0; i < numValues; i++) {
        auto value = ((T*)srcBuffer)[posInSrc + i];
        // Null values will usually be 0, which will not be able to be stored if there is a non-zero
        // offset However we don't care about the value stored for null values Currently they will
        // be mangled by storage+recovery (underflow in the subtraction below)
        KU_ASSERT((nullMask && nullMask->isNull(posInSrc + i)) || canUpdateInPlace(value, header));
        chunk[startPosInChunk] = (U)(value - (T)header.offset);
        if (startPosInChunk + 1 >= CHUNK_SIZE && i + 1 < numValues) {
            fastpack(chunk, chunkStart, header.bitWidth);
            chunkStart = (uint8_t*)getChunkStart(dstBuffer, posInDst + i + 1, header.bitWidth);
            fastunpack(chunkStart, chunk, header.bitWidth);
            startPosInChunk = 0;
        } else {
            startPosInChunk++;
        }
    }
    fastpack(chunk, chunkStart, header.bitWidth);
}

template<typename T>
void IntegerBitpacking<T>::getValues(const uint8_t* chunkStart, uint8_t pos, uint8_t* dst,
    uint8_t numValuesToRead, const BitpackHeader& header) const {
    // TODO(bmwinger): optimize as in setValueFromUncompressed
    KU_ASSERT(pos + numValuesToRead <= CHUNK_SIZE);

    U chunk[CHUNK_SIZE];
    fastunpack(chunkStart, chunk, header.bitWidth);
    if (header.hasNegative) {
        SignExtend<T, U, CHUNK_SIZE>((uint8_t*)chunk, header.bitWidth);
    }
    if (header.offset != 0) {
        for (int i = pos; i < pos + numValuesToRead; i++) {
            chunk[i] = (U)((T)chunk[i] + (T)header.offset);
        }
    }
    memcpy(dst, &chunk[pos], sizeof(T) * numValuesToRead);
}

template<typename T>
uint64_t IntegerBitpacking<T>::compressNextPage(const uint8_t*& srcBuffer,
    uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
    const struct CompressionMetadata& metadata) const {
    // TODO(bmwinger): this is hacky; we need a better system for dynamically choosing between
    // algorithms when compressing
    if (metadata.compression == CompressionType::UNCOMPRESSED) {
        return Uncompressed(sizeof(T)).compressNextPage(srcBuffer, numValuesRemaining, dstBuffer,
            dstBufferSize, metadata);
    }
    KU_ASSERT(metadata.compression == CompressionType::INTEGER_BITPACKING);
    auto header = BitpackHeader::readHeader(metadata.data);
    auto bitWidth = header.bitWidth;

    if (bitWidth == 0) {
        return 0;
    }
    auto numValuesToCompress = std::min(numValuesRemaining, numValues(dstBufferSize, header));
    // Round up to nearest byte
    auto sizeToCompress =
        numValuesToCompress * bitWidth / 8 + (numValuesToCompress * bitWidth % 8 != 0);
    KU_ASSERT(dstBufferSize >= CHUNK_SIZE);
    KU_ASSERT(dstBufferSize >= sizeToCompress);
    // This might overflow the source buffer if there are fewer values remaining than the chunk size
    // so we stop at the end of the last full chunk and use a temporary array to avoid overflow.
    if (header.offset == 0) {
        auto lastFullChunkEnd = numValuesToCompress - numValuesToCompress % CHUNK_SIZE;
        for (auto i = 0ull; i < lastFullChunkEnd; i += CHUNK_SIZE) {
            fastpack((const U*)srcBuffer + i, dstBuffer + i * bitWidth / 8, bitWidth);
        }
        // Pack last partial chunk, avoiding overflows
        if (numValuesToCompress % CHUNK_SIZE > 0) {
            // TODO(bmwinger): optimize to remove temporary array
            U chunk[CHUNK_SIZE] = {0};
            memcpy(chunk, (const U*)srcBuffer + lastFullChunkEnd,
                numValuesToCompress % CHUNK_SIZE * sizeof(U));
            fastpack(chunk, dstBuffer + lastFullChunkEnd * bitWidth / 8, bitWidth);
        }
    } else {
        U tmp[CHUNK_SIZE];
        auto lastFullChunkEnd = numValuesToCompress - numValuesToCompress % CHUNK_SIZE;
        for (auto i = 0ull; i < lastFullChunkEnd; i += CHUNK_SIZE) {
            for (auto j = 0u; j < CHUNK_SIZE; j++) {
                tmp[j] = (U)(((T*)srcBuffer)[i + j] - (T)header.offset);
            }
            fastpack(tmp, dstBuffer + i * bitWidth / 8, bitWidth);
        }
        // Pack last partial chunk, avoiding overflows
        auto remainingValues = numValuesToCompress % CHUNK_SIZE;
        if (remainingValues > 0) {
            memcpy(tmp, (const U*)srcBuffer + lastFullChunkEnd, remainingValues * sizeof(U));
            for (auto i = 0u; i < remainingValues; i++) {
                tmp[i] = (U)((T)tmp[i] - (T)header.offset);
            }
            memset(tmp + remainingValues, 0, CHUNK_SIZE - remainingValues);
            fastpack(tmp, dstBuffer + lastFullChunkEnd * bitWidth / 8, bitWidth);
        }
    }
    srcBuffer += numValuesToCompress * sizeof(U);
    return sizeToCompress;
}

template<typename T>
void IntegerBitpacking<T>::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& metadata) const {
    auto header = BitpackHeader::readHeader(metadata.data);

    auto srcCursor = getChunkStart(srcBuffer, srcOffset, header.bitWidth);
    auto valuesInFirstChunk = std::min(CHUNK_SIZE - (srcOffset % CHUNK_SIZE), numValues);
    auto bytesPerChunk = CHUNK_SIZE / 8 * header.bitWidth;
    auto dstIndex = dstOffset;

    // Copy values which aren't aligned to the start of the chunk
    if (valuesInFirstChunk < CHUNK_SIZE) {
        getValues(srcCursor, srcOffset % CHUNK_SIZE, dstBuffer + dstIndex * sizeof(U),
            valuesInFirstChunk, header);
        if (numValues == valuesInFirstChunk) {
            return;
        }
        // Start at the end of the first partial chunk
        srcCursor += bytesPerChunk;
        dstIndex += valuesInFirstChunk;
    }

    // Use fastunpack to directly unpack the full-sized chunks
    for (; dstIndex + CHUNK_SIZE <= dstOffset + numValues; dstIndex += CHUNK_SIZE) {
        fastunpack(srcCursor, (U*)dstBuffer + dstIndex, header.bitWidth);
        if (header.hasNegative) {
            SignExtend<T, U, CHUNK_SIZE>(dstBuffer + dstIndex * sizeof(U), header.bitWidth);
        }
        if (header.offset != 0) {
            for (auto i = 0u; i < CHUNK_SIZE; i++) {
                ((T*)dstBuffer)[dstIndex + i] += (T)header.offset;
            }
        }
        srcCursor += bytesPerChunk;
    }
    // Copy remaining values from within the last chunk.
    if (dstIndex < dstOffset + numValues) {
        getValues(srcCursor, 0, dstBuffer + dstIndex * sizeof(U), dstOffset + numValues - dstIndex,
            header);
    }
}

template class IntegerBitpacking<int8_t>;
template class IntegerBitpacking<int16_t>;
template class IntegerBitpacking<int32_t>;
template class IntegerBitpacking<int64_t>;
template class IntegerBitpacking<uint8_t>;
template class IntegerBitpacking<uint16_t>;
template class IntegerBitpacking<uint32_t>;
template class IntegerBitpacking<uint64_t>;

void BooleanBitpacking::setValuesFromUncompressed(const uint8_t* srcBuffer, offset_t srcOffset,
    uint8_t* dstBuffer, offset_t dstOffset, offset_t numValues,
    const CompressionMetadata& /*metadata*/, const NullMask* /*nullMask*/) const {
    for (auto i = 0u; i < numValues; i++) {
        NullMask::setNull((uint64_t*)dstBuffer, dstOffset + i, ((bool*)srcBuffer)[srcOffset + i]);
    }
}

uint64_t BooleanBitpacking::compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
    uint8_t* dstBuffer, uint64_t dstBufferSize,
    const struct CompressionMetadata& /*metadata*/) const {
    // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
    auto numValuesToCompress = std::min(numValuesRemaining, numValues(dstBufferSize));
    for (auto i = 0ull; i < numValuesToCompress; i++) {
        NullMask::setNull((uint64_t*)dstBuffer, i, srcBuffer[i]);
    }
    srcBuffer += numValuesToCompress / 8;
    // Will be a multiple of 8 except for the last iteration
    return numValuesToCompress / 8 + (bool)(numValuesToCompress % 8);
}

void BooleanBitpacking::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& /*metadata*/) const {
    // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
    for (auto i = 0ull; i < numValues; i++) {
        ((bool*)dstBuffer)[dstOffset + i] = NullMask::isNull((uint64_t*)srcBuffer, srcOffset + i);
    }
}

void BooleanBitpacking::copyFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& /*metadata*/) const {
    NullMask::copyNullMask(reinterpret_cast<const uint64_t*>(srcBuffer), srcOffset,
        reinterpret_cast<uint64_t*>(dstBuffer), dstOffset, numValues);
}

std::array<uint8_t, CompressionMetadata::DATA_SIZE> BitpackHeader::getData() const {
    std::array<uint8_t, CompressionMetadata::DATA_SIZE> data = {bitWidth};
    *(uint64_t*)&data[1] = offset;
    if (hasNegative) {
        data[0] |= NEGATIVE_FLAG;
    }
    return data;
}

BitpackHeader BitpackHeader::readHeader(
    const std::array<uint8_t, CompressionMetadata::DATA_SIZE>& data) {
    BitpackHeader header;
    header.bitWidth = data[0] & BITWIDTH_MASK;
    header.hasNegative = data[0] & NEGATIVE_FLAG;
    header.offset = *(uint64_t*)&data[1];
    return header;
}

void ReadCompressedValuesFromPageToVector::operator()(const uint8_t* frame, PageCursor& pageCursor,
    common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
    const CompressionMetadata& metadata) {
    switch (metadata.compression) {
    case CompressionType::CONSTANT:
        return constant.decompressFromPage(frame, pageCursor.elemPosInPage, resultVector->getData(),
            posInVector, numValuesToRead, metadata);
    case CompressionType::UNCOMPRESSED:
        return uncompressed.decompressFromPage(frame, pageCursor.elemPosInPage,
            resultVector->getData(), posInVector, numValuesToRead, metadata);
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            return IntegerBitpacking<int64_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT32: {
            return IntegerBitpacking<int32_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT16: {
            return IntegerBitpacking<int16_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT8: {
            return IntegerBitpacking<int8_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INTERNAL_ID:
        case PhysicalTypeID::ARRAY:
        case PhysicalTypeID::LIST:
        case PhysicalTypeID::UINT64: {
            return IntegerBitpacking<uint64_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::STRING:
        case PhysicalTypeID::UINT32: {
            return IntegerBitpacking<uint32_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::UINT16: {
            return IntegerBitpacking<uint16_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::UINT8: {
            return IntegerBitpacking<uint8_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        default: {
            throw NotImplementedException("INTEGER_BITPACKING is not implemented for type " +
                                          PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING:
        return booleanBitpacking.decompressFromPage(frame, pageCursor.elemPosInPage,
            resultVector->getData(), posInVector, numValuesToRead, metadata);
    default:
        KU_UNREACHABLE;
    }
}

void ReadCompressedValuesFromPage::operator()(const uint8_t* frame, PageCursor& pageCursor,
    uint8_t* result, uint32_t startPosInResult, uint64_t numValuesToRead,
    const CompressionMetadata& metadata) {
    switch (metadata.compression) {
    case CompressionType::CONSTANT:
        return constant.copyFromPage(frame, pageCursor.elemPosInPage, result, startPosInResult,
            numValuesToRead, metadata);
    case CompressionType::UNCOMPRESSED:
        return uncompressed.decompressFromPage(frame, pageCursor.elemPosInPage, result,
            startPosInResult, numValuesToRead, metadata);
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            return IntegerBitpacking<int64_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT32: {
            return IntegerBitpacking<int32_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT16: {
            return IntegerBitpacking<int16_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT8: {
            return IntegerBitpacking<int8_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INTERNAL_ID:
        case PhysicalTypeID::ARRAY:
        case PhysicalTypeID::LIST:
        case PhysicalTypeID::UINT64: {
            return IntegerBitpacking<uint64_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::STRING:
        case PhysicalTypeID::UINT32: {
            return IntegerBitpacking<uint32_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::UINT16: {
            return IntegerBitpacking<uint16_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::UINT8: {
            return IntegerBitpacking<int8_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        default: {
            throw NotImplementedException("INTEGER_BITPACKING is not implemented for type " +
                                          PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING:
        // Reading into ColumnChunks should be done without decompressing for booleans
        return booleanBitpacking.copyFromPage(frame, pageCursor.elemPosInPage, result,
            startPosInResult, numValuesToRead, metadata);
    default:
        KU_UNREACHABLE;
    }
}

void WriteCompressedValuesToPage::operator()(uint8_t* frame, uint16_t posInFrame,
    const uint8_t* data, offset_t dataOffset, offset_t numValues,
    const CompressionMetadata& metadata, const NullMask* nullMask) {
    switch (metadata.compression) {
    case CompressionType::CONSTANT:
        return constant.setValuesFromUncompressed(data, dataOffset, frame, posInFrame, numValues,
            metadata, nullMask);
    case CompressionType::UNCOMPRESSED:
        return uncompressed.setValuesFromUncompressed(data, dataOffset, frame, posInFrame,
            numValues, metadata, nullMask);
    case CompressionType::INTEGER_BITPACKING: {
        return TypeUtils::visit(physicalType, [&]<typename T>(T) {
            if constexpr (std::same_as<T, bool>) {
                throw NotImplementedException(
                    "INTEGER_BITPACKING is not implemented for type bool");
            } else if constexpr (std::same_as<T, internalID_t> || std::same_as<T, list_entry_t>) {
                IntegerBitpacking<uint64_t>().setValuesFromUncompressed(data, dataOffset, frame,
                    posInFrame, numValues, metadata, nullMask);
            } else if constexpr (std::integral<T>) {
                return IntegerBitpacking<T>().setValuesFromUncompressed(data, dataOffset, frame,
                    posInFrame, numValues, metadata, nullMask);
            } else if constexpr (std::same_as<T, ku_string_t>) {
                return IntegerBitpacking<uint32_t>().setValuesFromUncompressed(data, dataOffset,
                    frame, posInFrame, numValues, metadata, nullMask);
            } else {
                throw NotImplementedException(
                    "INTEGER_BITPACKING is not implemented for type " +
                    PhysicalTypeUtils::physicalTypeToString(physicalType));
            }
        });
    }
    case CompressionType::BOOLEAN_BITPACKING:
        return booleanBitpacking.copyFromPage(data, dataOffset, frame, posInFrame, numValues,
            metadata);

    default:
        KU_UNREACHABLE;
    }
}

void WriteCompressedValuesToPage::operator()(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, const CompressionMetadata& metadata) {
    if (metadata.compression == CompressionType::BOOLEAN_BITPACKING) {
        booleanBitpacking.setValuesFromUncompressed(vector->getData(), posInVector, frame,
            posInFrame, 1, metadata, &vector->getNullMask());
    } else {
        (*this)(frame, posInFrame, vector->getData(), posInVector, 1, metadata,
            &vector->getNullMask());
    }
}

} // namespace storage
} // namespace kuzu
