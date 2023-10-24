#include "processor/operator/persistent/reader/parquet/column_reader.h"

#include "common/exception/not_implemented.h"
#include "common/types/date_t.h"
#include "processor/operator/persistent/reader/parquet/boolean_column_reader.h"
#include "processor/operator/persistent/reader/parquet/callback_column_reader.h"
#include "processor/operator/persistent/reader/parquet/string_column_reader.h"
#include "processor/operator/persistent/reader/parquet/templated_column_reader.h"
#include "snappy/snappy.h"

namespace kuzu {
namespace processor {

using kuzu_parquet::format::CompressionCodec;
using kuzu_parquet::format::ConvertedType;
using kuzu_parquet::format::Encoding;
using kuzu_parquet::format::PageType;
using kuzu_parquet::format::Type;

static common::date_t ParquetIntToDate(const int32_t& raw_date) {
    return common::date_t(raw_date);
}

ColumnReader::ColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
    const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefinition,
    uint64_t maxRepeat)
    : schema{schema}, fileIdx{fileIdx}, maxDefine{maxDefinition}, maxRepeat{maxRepeat},
      reader{reader}, type{std::move(type)}, pageRowsAvailable{0} {}

void ColumnReader::initializeRead(uint64_t /*rowGroupIdx*/,
    const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
    kuzu_apache::thrift::protocol::TProtocol& protocol) {
    assert(fileIdx < columns.size());
    chunk = &columns[fileIdx];
    this->protocol = &protocol;
    assert(chunk);
    assert(chunk->__isset.meta_data);

    if (chunk->__isset.file_path) {
        throw std::runtime_error("Only inlined data files are supported (no references)");
    }

    // ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
    chunkReadOffset = chunk->meta_data.data_page_offset;
    if (chunk->meta_data.__isset.dictionary_page_offset &&
        chunk->meta_data.dictionary_page_offset >= 4) {
        // this assumes the data pages follow the dict pages directly.
        chunkReadOffset = chunk->meta_data.dictionary_page_offset;
    }
    groupRowsAvailable = chunk->meta_data.num_values;
}

void ColumnReader::registerPrefetch(ThriftFileTransport& transport, bool allowMerge) {
    if (chunk) {
        uint64_t size = chunk->meta_data.total_compressed_size;
        transport.RegisterPrefetch(fileOffset(), size, allowMerge);
    }
}

uint64_t ColumnReader::fileOffset() const {
    if (!chunk) {
        throw std::runtime_error("fileOffset called on ColumnReader with no chunk");
    }
    auto minOffset = UINT64_MAX;
    if (chunk->meta_data.__isset.dictionary_page_offset) {
        minOffset = std::min<uint64_t>(minOffset, chunk->meta_data.dictionary_page_offset);
    }
    if (chunk->meta_data.__isset.index_page_offset) {
        minOffset = std::min<uint64_t>(minOffset, chunk->meta_data.index_page_offset);
    }
    minOffset = std::min<uint64_t>(minOffset, chunk->meta_data.data_page_offset);

    return minOffset;
}

void ColumnReader::applyPendingSkips(uint64_t numValues) {
    pendingSkips -= numValues;

    dummyDefine.zero();
    dummyRepeat.zero();

    // TODO this can be optimized, for example we dont actually have to bitunpack offsets
    std::unique_ptr<common::ValueVector> dummyResult =
        std::make_unique<common::ValueVector>(*type->copy());

    uint64_t remaining = numValues;
    uint64_t numValuesRead = 0;

    while (remaining) {
        auto numValuesToRead = std::min<uint64_t>(remaining, common::DEFAULT_VECTOR_CAPACITY);
        numValuesRead +=
            read(numValuesToRead, noneFilter, dummyDefine.ptr, dummyRepeat.ptr, dummyResult.get());
        remaining -= numValuesToRead;
    }

    if (numValuesRead != numValues) {
        throw std::runtime_error("Row count mismatch when skipping rows");
    }
}

uint64_t ColumnReader::read(uint64_t numValues, parquet_filter_t& filter, uint8_t* defineOut,
    uint8_t* repeatOut, common::ValueVector* resultOut) {
    // we need to reset the location because multiple column readers share the same protocol
    auto& trans = reinterpret_cast<ThriftFileTransport&>(*protocol->getTransport());
    trans.SetLocation(chunkReadOffset);

    // Perform any skips that were not applied yet.
    if (pendingSkips > 0) {
        applyPendingSkips(pendingSkips);
    }

    uint64_t resultOffset = 0;
    auto toRead = numValues;

    while (toRead > 0) {
        while (pageRowsAvailable == 0) {
            prepareRead(filter);
        }

        assert(block);
        auto readNow = std::min<uint64_t>(toRead, pageRowsAvailable);

        assert(readNow <= common::DEFAULT_VECTOR_CAPACITY);

        if (hasRepeats()) {
            assert(repeatedDecoder);
            repeatedDecoder->GetBatch<uint8_t>(repeatOut + resultOffset, readNow);
        }

        if (hasDefines()) {
            assert(defineDecoder);
            defineDecoder->GetBatch<uint8_t>(defineOut + resultOffset, readNow);
        }

        uint64_t nullCount = 0;

        if ((dictDecoder || dbpDecoder || rleDecoder) && hasDefines()) {
            // we need the null count because the dictionary offsets have no entries for nulls
            for (auto i = 0u; i < readNow; i++) {
                if (defineOut[i + resultOffset] != maxDefine) {
                    nullCount++;
                }
            }
        }

        if (dictDecoder) {
            offsetBuffer.resize(sizeof(uint32_t) * (readNow - nullCount));
            dictDecoder->GetBatch<uint32_t>(offsetBuffer.ptr, readNow - nullCount);
            offsets(reinterpret_cast<uint32_t*>(offsetBuffer.ptr), defineOut, readNow, filter,
                resultOffset, resultOut);
        } else if (dbpDecoder) {
            // TODO keep this in the state
            auto readBuf = std::make_shared<ResizeableBuffer>();

            switch (type->getPhysicalType()) {
            case common::PhysicalTypeID::INT32:
                readBuf->resize(sizeof(int32_t) * (readNow - nullCount));
                dbpDecoder->GetBatch<int32_t>(readBuf->ptr, readNow - nullCount);
                break;
            case common::PhysicalTypeID::INT64:
                readBuf->resize(sizeof(int64_t) * (readNow - nullCount));
                dbpDecoder->GetBatch<int64_t>(readBuf->ptr, readNow - nullCount);
                break;
            default:
                throw std::runtime_error("DELTA_BINARY_PACKED should only be INT32 or INT64");
            }
            // Plain() will put NULLs in the right place
            plain(readBuf, defineOut, readNow, filter, resultOffset, resultOut);
        } else if (rleDecoder) {
            // RLE encoding for boolean
            assert(type->getLogicalTypeID() == common::LogicalTypeID::BOOL);
            auto readBuf = std::make_shared<ResizeableBuffer>();
            readBuf->resize(sizeof(bool) * (readNow - nullCount));
            rleDecoder->GetBatch<uint8_t>(readBuf->ptr, readNow - nullCount);
            plainTemplated<bool, TemplatedParquetValueConversion<bool>>(
                readBuf, defineOut, readNow, filter, resultOffset, resultOut);
        } else {
            plain(block, defineOut, readNow, filter, resultOffset, resultOut);
        }

        resultOffset += readNow;
        pageRowsAvailable -= readNow;
        toRead -= readNow;
    }
    groupRowsAvailable -= numValues;
    chunkReadOffset = trans.GetLocation();

    return numValues;
}

std::unique_ptr<ColumnReader> ColumnReader::createReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type, const kuzu_parquet::format::SchemaElement& schema,
    uint64_t fileIdx, uint64_t maxDefine, uint64_t maxRepeat) {
    switch (type->getLogicalTypeID()) {
    case common::LogicalTypeID::BOOL:
        return std::make_unique<BooleanColumnReader>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT16:
        return std::make_unique<
            TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT32:
        return std::make_unique<
            TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT64:
        return std::make_unique<
            TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<int64_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::FLOAT:
        return std::make_unique<
            TemplatedColumnReader<float, TemplatedParquetValueConversion<float>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::DOUBLE:
        return std::make_unique<
            TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::DATE:
        return std::make_unique<CallbackColumnReader<int32_t, common::date_t, ParquetIntToDate>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::STRING:
        return std::make_unique<StringColumnReader>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    default:
        throw common::NotImplementedException{"ColumnReader::createReader"};
    }
}

void ColumnReader::prepareRead(parquet_filter_t& /*filter*/) {
    dictDecoder.reset();
    defineDecoder.reset();
    block.reset();
    kuzu_parquet::format::PageHeader pageHdr;
    pageHdr.read(protocol);

    switch (pageHdr.type) {
    case PageType::DATA_PAGE_V2:
        preparePageV2(pageHdr);
        prepareDataPage(pageHdr);
        break;
    case PageType::DATA_PAGE:
        preparePage(pageHdr);
        prepareDataPage(pageHdr);
        break;
    case PageType::DICTIONARY_PAGE:
        preparePage(pageHdr);
        dictionary(std::move(block), pageHdr.dictionary_page_header.num_values);
        break;
    default:
        break; // ignore INDEX page type and any other custom extensions
    }
    resetPage();
}

void ColumnReader::allocateBlock(uint64_t size) {
    if (!block) {
        block = std::make_shared<ResizeableBuffer>(size);
    } else {
        block->resize(size);
    }
}

void ColumnReader::allocateCompressed(uint64_t size) {
    compressedBuffer.resize(size);
}

void ColumnReader::decompressInternal(kuzu_parquet::format::CompressionCodec::type codec,
    const uint8_t* src, uint64_t srcSize, uint8_t* dst, uint64_t dstSize) {
    switch (codec) {
    case CompressionCodec::UNCOMPRESSED:
        throw common::CopyException("Parquet data unexpectedly uncompressed");
    case CompressionCodec::GZIP: {
        throw common::NotImplementedException("ColumnReader::decompressInternal");
    }
    case CompressionCodec::SNAPPY: {
        {
            size_t uncompressed_size = 0;
            auto res = kuzu_snappy::GetUncompressedLength(
                reinterpret_cast<const char*>(src), srcSize, &uncompressed_size);
            if (!res) {
                throw std::runtime_error("Snappy decompression failure");
            }
            if (uncompressed_size != (size_t)dstSize) {
                throw std::runtime_error(
                    "Snappy decompression failure: Uncompressed data size mismatch");
            }
        }
        auto res = kuzu_snappy::RawUncompress(
            reinterpret_cast<const char*>(src), srcSize, reinterpret_cast<char*>(dst));
        if (!res) {
            throw std::runtime_error("Snappy decompression failure");
        }
        break;
    }
    case CompressionCodec::ZSTD: {
        throw common::NotImplementedException("ColumnReader::decompressInternal");
    }
    default: {
        std::stringstream codec_name;
        codec_name << codec;
        throw common::CopyException("Unsupported compression codec \"" + codec_name.str() +
                                    "\". Supported options are uncompressed, gzip, snappy or zstd");
    }
    }
}

void ColumnReader::preparePageV2(kuzu_parquet::format::PageHeader& pageHdr) {
    assert(pageHdr.type == PageType::DATA_PAGE_V2);

    auto& trans = reinterpret_cast<ThriftFileTransport&>(*protocol->getTransport());

    allocateBlock(pageHdr.uncompressed_page_size + 1);
    bool uncompressed = false;
    if (pageHdr.data_page_header_v2.__isset.is_compressed &&
        !pageHdr.data_page_header_v2.is_compressed) {
        uncompressed = true;
    }
    if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
        if (pageHdr.compressed_page_size != pageHdr.uncompressed_page_size) {
            throw std::runtime_error("Page size mismatch");
        }
        uncompressed = true;
    }
    if (uncompressed) {
        trans.read(block->ptr, pageHdr.compressed_page_size);
        return;
    }

    // copy repeats & defines as-is because FOR SOME REASON they are uncompressed
    auto uncompressedBytes = pageHdr.data_page_header_v2.repetition_levels_byte_length +
                             pageHdr.data_page_header_v2.definition_levels_byte_length;
    trans.read(block->ptr, uncompressedBytes);

    auto compressedBytes = pageHdr.compressed_page_size - uncompressedBytes;

    allocateCompressed(compressedBytes);
    trans.read(compressedBuffer.ptr, compressedBytes);

    decompressInternal(chunk->meta_data.codec, compressedBuffer.ptr, compressedBytes,
        block->ptr + uncompressedBytes, pageHdr.uncompressed_page_size - uncompressedBytes);
}

void ColumnReader::preparePage(kuzu_parquet::format::PageHeader& pageHdr) {
    auto& trans = reinterpret_cast<ThriftFileTransport&>(*protocol->getTransport());

    allocateBlock(pageHdr.uncompressed_page_size + 1);
    if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
        if (pageHdr.compressed_page_size != pageHdr.uncompressed_page_size) {
            throw std::runtime_error("Page size mismatch");
        }
        trans.read((uint8_t*)block->ptr, pageHdr.compressed_page_size);
        return;
    }

    allocateCompressed(pageHdr.compressed_page_size + 1);
    trans.read((uint8_t*)compressedBuffer.ptr, pageHdr.compressed_page_size);

    decompressInternal(chunk->meta_data.codec, compressedBuffer.ptr, pageHdr.compressed_page_size,
        block->ptr, pageHdr.uncompressed_page_size);
}

void ColumnReader::prepareDataPage(kuzu_parquet::format::PageHeader& pageHdr) {
    if (pageHdr.type == PageType::DATA_PAGE && !pageHdr.__isset.data_page_header) {
        throw std::runtime_error("Missing data page header from data page");
    }
    if (pageHdr.type == PageType::DATA_PAGE_V2 && !pageHdr.__isset.data_page_header_v2) {
        throw std::runtime_error("Missing data page header from data page v2");
    }

    bool isV1 = pageHdr.type == PageType::DATA_PAGE;
    bool isV2 = pageHdr.type == PageType::DATA_PAGE_V2;
    auto& v1Header = pageHdr.data_page_header;
    auto& v2Header = pageHdr.data_page_header_v2;

    pageRowsAvailable = isV1 ? v1Header.num_values : v2Header.num_values;
    auto pageEncoding = isV1 ? v1Header.encoding : v2Header.encoding;

    if (hasRepeats()) {
        uint32_t repLength =
            isV1 ? block->read<uint32_t>() : v2Header.repetition_levels_byte_length;
        block->available(repLength);
        repeatedDecoder = std::make_unique<RleBpDecoder>(
            block->ptr, repLength, RleBpDecoder::ComputeBitWidth(maxRepeat));
        block->inc(repLength);
    } else if (isV2 && v2Header.repetition_levels_byte_length > 0) {
        block->inc(v2Header.repetition_levels_byte_length);
    }

    if (hasDefines()) {
        auto defLen = isV1 ? block->read<uint32_t>() : v2Header.definition_levels_byte_length;
        block->available(defLen);
        defineDecoder = std::make_unique<RleBpDecoder>(
            block->ptr, defLen, RleBpDecoder::ComputeBitWidth(maxDefine));
        block->inc(defLen);
    } else if (isV2 && v2Header.definition_levels_byte_length > 0) {
        block->inc(v2Header.definition_levels_byte_length);
    }

    switch (pageEncoding) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY: {
        // where is it otherwise??
        auto dictWidth = block->read<uint8_t>();
        // TODO somehow dict_width can be 0 ?
        dictDecoder = std::make_unique<RleBpDecoder>(block->ptr, block->len, dictWidth);
        block->inc(block->len);
        break;
    }
    case Encoding::RLE: {
        if (type->getLogicalTypeID() != common::LogicalTypeID::BOOL) {
            throw std::runtime_error("RLE encoding is only supported for boolean data");
        }
        block->inc(sizeof(uint32_t));
        rleDecoder = std::make_unique<RleBpDecoder>(block->ptr, block->len, 1);
        break;
    }
    case Encoding::DELTA_BINARY_PACKED: {
        dbpDecoder = std::make_unique<DbpDecoder>(block->ptr, block->len);
        block->inc(block->len);
        break;
    }
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
    case Encoding::DELTA_BYTE_ARRAY: {
        throw common::NotImplementedException("ColumnReader::prepareDataPage");
    }
    case Encoding::PLAIN:
        // nothing to do here, will be read directly below
        break;
    default:
        throw std::runtime_error("Unsupported page encoding");
    }
}

uint64_t ColumnReader::getTotalCompressedSize() {
    if (!chunk) {
        return 0;
    }
    return chunk->meta_data.total_compressed_size;
}

const uint64_t ParquetDecodeUtils::BITPACK_MASKS[] = {0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 1023,
    2047, 4095, 8191, 16383, 32767, 65535, 131071, 262143, 524287, 1048575, 2097151, 4194303,
    8388607, 16777215, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647,
    4294967295, 8589934591, 17179869183, 34359738367, 68719476735, 137438953471, 274877906943,
    549755813887, 1099511627775, 2199023255551, 4398046511103, 8796093022207, 17592186044415,
    35184372088831, 70368744177663, 140737488355327, 281474976710655, 562949953421311,
    1125899906842623, 2251799813685247, 4503599627370495, 9007199254740991, 18014398509481983,
    36028797018963967, 72057594037927935, 144115188075855871, 288230376151711743,
    576460752303423487, 1152921504606846975, 2305843009213693951, 4611686018427387903,
    9223372036854775807, 18446744073709551615ULL};

const uint64_t ParquetDecodeUtils::BITPACK_MASKS_SIZE =
    sizeof(ParquetDecodeUtils::BITPACK_MASKS) / sizeof(uint64_t);

const uint8_t ParquetDecodeUtils::BITPACK_DLEN = 8;

} // namespace processor
} // namespace kuzu
