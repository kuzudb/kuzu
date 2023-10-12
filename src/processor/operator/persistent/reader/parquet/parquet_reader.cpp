#include "processor/operator/persistent/reader/parquet/parquet_reader.h"

#include "common/exception/copy.h"
#include "common/exception/not_implemented.h"
#include "common/file_utils.h"
#include "common/string_format.h"
#include "processor/operator/persistent/reader/parquet/list_column_reader.h"
#include "processor/operator/persistent/reader/parquet/thrift_tools.h"

using namespace kuzu_parquet::format;

namespace kuzu {
namespace processor {

ParquetReader::ParquetReader(const std::string& filePath, storage::MemoryManager* memoryManager)
    : filePath{filePath}, memoryManager{memoryManager} {
    fileInfo = common::FileUtils::openFile(filePath, O_RDONLY);
    initMetadata();
}

void ParquetReader::initializeScan(
    ParquetReaderScanState& state, std::vector<uint64_t> groups_to_read) {
    state.currentGroup = -1;
    state.finished = false;
    state.groupOffset = 0;
    state.groupIdxList = std::move(groups_to_read);
    if (!state.fileInfo || state.fileInfo->path != fileInfo->path) {
        state.prefetchMode = false;
        state.fileInfo = common::FileUtils::openFile(fileInfo->path, O_RDONLY);
    }

    state.thriftFileProto = createThriftProtocol(state.fileInfo.get(), state.prefetchMode);
    state.rootReader = createReader();
    state.defineBuf.resize(common::DEFAULT_VECTOR_CAPACITY);
    state.repeatBuf.resize(common::DEFAULT_VECTOR_CAPACITY);
}

bool ParquetReader::scanInternal(ParquetReaderScanState& state, common::DataChunk& result) {
    if (state.finished) {
        return false;
    }

    // see if we have to switch to the next row group in the parquet file
    if (state.currentGroup < 0 || (int64_t)state.groupOffset >= getGroup(state).num_rows) {
        state.currentGroup++;
        state.groupOffset = 0;

        auto& trans =
            reinterpret_cast<ThriftFileTransport&>(*state.thriftFileProto->getTransport());
        trans.ClearPrefetch();
        state.currentGroupPrefetched = false;

        if ((uint64_t)state.currentGroup == state.groupIdxList.size()) {
            state.finished = true;
            return false;
        }

        uint64_t toScanCompressedBytes = 0;
        for (auto colIdx = 0u; colIdx < result.getNumValueVectors(); colIdx++) {
            prepareRowGroupBuffer(state, colIdx);

            auto fileColIdx = colIdx;

            auto rootReader = reinterpret_cast<StructColumnReader*>(state.rootReader.get());
            toScanCompressedBytes +=
                rootReader->getChildReader(fileColIdx)->getTotalCompressedSize();
        }

        auto& group = getGroup(state);
        if (state.prefetchMode && state.groupOffset != (uint64_t)group.num_rows) {

            uint64_t totalRowGroupSpan = getGroupSpan(state);

            double scanPercentage = (double)(toScanCompressedBytes) / totalRowGroupSpan;

            if (toScanCompressedBytes > totalRowGroupSpan) {
                throw common::CopyException("Malformed parquet file: sum of total compressed bytes "
                                            "of columns seems incorrect");
            }

            if (scanPercentage > ParquetReaderPrefetchConfig::WHOLE_GROUP_PREFETCH_MINIMUM_SCAN) {
                // Prefetch the whole row group
                if (!state.currentGroupPrefetched) {
                    auto totalCompressedSize = getGroupCompressedSize(state);
                    if (totalCompressedSize > 0) {
                        trans.Prefetch(getGroupOffset(state), totalRowGroupSpan);
                    }
                    state.currentGroupPrefetched = true;
                }
            } else {
                // lazy fetching is when all tuples in a column can be skipped. With lazy fetching
                // the buffer is only fetched on the first read to that buffer.
                bool lazyFetch = false;

                // Prefetch column-wise
                for (auto colIdx = 0u; colIdx < result.getNumValueVectors(); colIdx++) {
                    auto fileColIdx = colIdx;
                    auto rootReader = reinterpret_cast<StructColumnReader*>(state.rootReader.get());

                    bool hasFilter = false;
                    rootReader->getChildReader(fileColIdx)
                        ->registerPrefetch(trans, !(lazyFetch && !hasFilter));
                }

                trans.FinalizeRegistration();

                if (!lazyFetch) {
                    trans.PrefetchRegistered();
                }
            }
        }
        return true;
    }

    auto thisOutputChunkRows = std::min<uint64_t>(
        common::DEFAULT_VECTOR_CAPACITY, getGroup(state).num_rows - state.groupOffset);
    result.state->selVector->selectedSize = thisOutputChunkRows;

    if (thisOutputChunkRows == 0) {
        state.finished = true;
        return false; // end of last group, we are done
    }

    // we evaluate simple table filters directly in this scan so we can skip decoding column data
    // that's never going to be relevant
    parquet_filter_t filterMask;
    filterMask.set();

    // mask out unused part of bitset
    for (auto i = thisOutputChunkRows; i < common::DEFAULT_VECTOR_CAPACITY; i++) {
        filterMask.set(i, false);
    }

    state.defineBuf.zero();
    state.repeatBuf.zero();

    auto definePtr = (uint8_t*)state.defineBuf.ptr;
    auto repeatPtr = (uint8_t*)state.repeatBuf.ptr;

    auto rootReader = reinterpret_cast<StructColumnReader*>(state.rootReader.get());

    for (auto colIdx = 0u; colIdx < result.getNumValueVectors(); colIdx++) {
        auto fileColIdx = colIdx;
        auto resultVector = result.getValueVector(colIdx);
        auto childReader = rootReader->getChildReader(fileColIdx);
        auto rowsRead = childReader->read(resultVector->state->selVector->selectedSize, filterMask,
            definePtr, repeatPtr, resultVector.get());
        if (rowsRead != result.state->selVector->selectedSize) {
            throw common::CopyException(common::stringFormat(
                "Mismatch in parquet read for column {}, expected {} rows, got {}", fileColIdx,
                result.state->selVector->selectedSize, rowsRead));
        }
    }

    state.groupOffset += thisOutputChunkRows;
    return true;
}

void ParquetReader::scan(processor::ParquetReaderScanState& state, common::DataChunk& result) {
    while (scanInternal(state, result)) {
        if (result.state->selVector->selectedSize > 0) {
            break;
        }
    }
}

void ParquetReader::initMetadata() {
    auto proto = createThriftProtocol(fileInfo.get(), false);
    auto& transport = reinterpret_cast<ThriftFileTransport&>(*proto->getTransport());
    auto fileSize = transport.GetSize();
    if (fileSize < 12) {
        throw common::CopyException{common::stringFormat(
            "File {} is too small to be a Parquet file", fileInfo->path.c_str())};
    }

    ResizeableBuffer buf;
    buf.resize(8);
    buf.zero();

    transport.SetLocation(fileSize - 8);
    transport.read((uint8_t*)buf.ptr, 8);

    if (memcmp(buf.ptr + 4, "PAR1", 4) != 0) {
        if (memcmp(buf.ptr + 4, "PARE", 4) == 0) {
            throw common::CopyException{common::stringFormat(
                "Encrypted Parquet files are not supported for file {}", fileInfo->path.c_str())};
        }
        throw common::CopyException{common::stringFormat(
            "No magic bytes found at the end of file {}", fileInfo->path.c_str())};
    }
    // read four-byte footer length from just before the end magic bytes
    auto footerLen = *reinterpret_cast<uint32_t*>(buf.ptr);
    if (footerLen == 0 || fileSize < 12 + footerLen) {
        throw common::CopyException{
            common::stringFormat("Footer length error in file {}", fileInfo->path.c_str())};
    }
    auto metadataPos = fileSize - (footerLen + 8);
    transport.SetLocation(metadataPos);
    transport.Prefetch(metadataPos, footerLen);

    metadata = std::make_unique<FileMetaData>();
    metadata->read(proto.get());
}

std::unique_ptr<ColumnReader> ParquetReader::createReaderRecursive(uint64_t depth,
    uint64_t maxDefine, uint64_t maxRepeat, uint64_t& next_schema_idx, uint64_t& next_file_idx) {
    assert(next_schema_idx < metadata->schema.size());
    auto& sEle = metadata->schema[next_schema_idx];
    auto thisIdx = next_schema_idx;

    auto repetition_type = FieldRepetitionType::REQUIRED;
    if (sEle.__isset.repetition_type && thisIdx > 0) {
        repetition_type = sEle.repetition_type;
    }
    if (repetition_type != FieldRepetitionType::REQUIRED) {
        maxDefine++;
    }
    if (repetition_type == FieldRepetitionType::REPEATED) {
        maxRepeat++;
    }
    if (sEle.__isset.num_children && sEle.num_children > 0) { // inner node
        std::vector<std::unique_ptr<common::StructField>> structFields;
        std::vector<std::unique_ptr<ColumnReader>> childrenReaders;

        uint64_t cIdx = 0;
        while (cIdx < (uint64_t)sEle.num_children) {
            next_schema_idx++;

            auto& child_ele = metadata->schema[next_schema_idx];

            auto child_reader = createReaderRecursive(
                depth + 1, maxDefine, maxRepeat, next_schema_idx, next_file_idx);
            structFields.push_back(std::make_unique<common::StructField>(
                child_ele.name, child_reader->getDataType()->copy()));
            childrenReaders.push_back(std::move(child_reader));

            cIdx++;
        }
        assert(!structFields.empty());
        std::unique_ptr<ColumnReader> result;
        std::unique_ptr<common::LogicalType> resultType;

        bool isRepeated = repetition_type == FieldRepetitionType::REPEATED;
        bool isList = sEle.__isset.converted_type && sEle.converted_type == ConvertedType::LIST;
        bool isMap = sEle.__isset.converted_type && sEle.converted_type == ConvertedType::MAP;
        if (structFields.size() > 1 || (!isList && !isMap && !isRepeated)) {
            resultType = std::make_unique<common::LogicalType>(common::LogicalTypeID::STRUCT,
                std::make_unique<common::StructTypeInfo>(std::move(structFields)));
            result = std::make_unique<StructColumnReader>(*this, std::move(resultType), sEle,
                thisIdx, maxDefine, maxRepeat, std::move(childrenReaders));
        } else {
            // if we have a struct with only a single type, pull up
            resultType = structFields[0]->getType()->copy();
            result = std::move(childrenReaders[0]);
        }
        if (isRepeated) {
            auto varListInfo = std::make_unique<common::VarListTypeInfo>(std::move(resultType));
            resultType = std::make_unique<common::LogicalType>(
                common::LogicalTypeID::VAR_LIST, std::move(varListInfo));
            return std::make_unique<ListColumnReader>(*this, std::move(resultType), sEle, thisIdx,
                maxDefine, maxRepeat, std::move(result), memoryManager);
        }
        return result;
    } else { // leaf node
        if (!sEle.__isset.type) {
            throw common::CopyException{"Node has neither num_children nor type set - this "
                                        "violates the Parquet spec (corrupted file)"};
        }
        if (sEle.repetition_type == FieldRepetitionType::REPEATED) {
            auto derivedType = deriveLogicalType(sEle);
            auto varListTypeInfo = std::make_unique<common::VarListTypeInfo>(derivedType->copy());
            auto listType = std::make_unique<common::LogicalType>(
                common::LogicalTypeID::VAR_LIST, std::move(varListTypeInfo));
            auto elementReader = ColumnReader::createReader(
                *this, std::move(derivedType), sEle, next_file_idx++, maxDefine, maxRepeat);
            return std::make_unique<ListColumnReader>(*this, std::move(listType), sEle, thisIdx,
                maxDefine, maxRepeat, std::move(elementReader), memoryManager);
        }
        // TODO check return value of derive type or should we only do this on read()
        return ColumnReader::createReader(
            *this, deriveLogicalType(sEle), sEle, next_file_idx++, maxDefine, maxRepeat);
    }
}

std::unique_ptr<ColumnReader> ParquetReader::createReader() {
    uint64_t nextSchemaIdx = 0;
    uint64_t nextFileIdx = 0;

    if (metadata->schema.empty()) {
        throw common::CopyException{"Parquet reader: no schema elements found"};
    }
    if (metadata->schema[0].num_children == 0) {
        throw common::CopyException{"Parquet reader: root schema element has no children"};
    }
    auto rootReader = createReaderRecursive(0, 0, 0, nextSchemaIdx, nextFileIdx);
    if (rootReader->getDataType()->getPhysicalType() != common::PhysicalTypeID::STRUCT) {
        throw common::CopyException{"Root element of Parquet file must be a struct"};
    }
    for (auto& field : common::StructType::getFields(rootReader->getDataType())) {
        columnNames.push_back(field->getName());
        columnTypes.push_back(field->getType()->copy());
    }

    assert(nextSchemaIdx == metadata->schema.size() - 1);
    assert(metadata->row_groups.empty() || nextFileIdx == metadata->row_groups[0].columns.size());
    return rootReader;
}

void ParquetReader::prepareRowGroupBuffer(ParquetReaderScanState& state, uint64_t col_idx) {
    auto& group = getGroup(state);
    auto columnIdx = col_idx;
    state.rootReader->initializeRead(
        state.groupIdxList[state.currentGroup], group.columns, *state.thriftFileProto);
}

uint64_t ParquetReader::getGroupSpan(ParquetReaderScanState& state) {
    auto& group = getGroup(state);
    uint64_t min_offset = UINT64_MAX;
    uint64_t max_offset = 0;

    for (auto& column_chunk : group.columns) {

        // Set the min offset
        auto current_min_offset = UINT64_MAX;
        if (column_chunk.meta_data.__isset.dictionary_page_offset) {
            current_min_offset = std::min<uint64_t>(
                current_min_offset, column_chunk.meta_data.dictionary_page_offset);
        }
        if (column_chunk.meta_data.__isset.index_page_offset) {
            current_min_offset =
                std::min<uint64_t>(current_min_offset, column_chunk.meta_data.index_page_offset);
        }
        current_min_offset =
            std::min<uint64_t>(current_min_offset, column_chunk.meta_data.data_page_offset);
        min_offset = std::min<uint64_t>(current_min_offset, min_offset);
        max_offset = std::max<uint64_t>(
            max_offset, column_chunk.meta_data.total_compressed_size + current_min_offset);
    }

    return max_offset - min_offset;
}

std::unique_ptr<common::LogicalType> ParquetReader::deriveLogicalType(
    const kuzu_parquet::format::SchemaElement& s_ele) {
    // inner node
    if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
        throw common::CopyException("FIXED_LEN_BYTE_ARRAY requires length to be set");
    }
    if (s_ele.__isset.converted_type) {
        switch (s_ele.converted_type) {
        case ConvertedType::INT_8:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT8);
            } else {
                throw common::CopyException{
                    "INT8 converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::INT_16:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT16);
            } else {
                throw common::CopyException{
                    "INT16 converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::INT_32:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT32);
            } else {
                throw common::CopyException{
                    "INT32 converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::INT_64:
            if (s_ele.type == Type::INT64) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT64);
            } else {
                throw common::CopyException{
                    "INT64 converted type can only be set for value of Type::INT64"};
            }
        case ConvertedType::DATE:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::DATE);
            } else {
                throw common::CopyException{
                    "DATE converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::UTF8:
        case ConvertedType::ENUM:
            switch (s_ele.type) {
            case Type::BYTE_ARRAY:
            case Type::FIXED_LEN_BYTE_ARRAY:
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::STRING);
            default:
                throw common::CopyException(
                    "UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
            }
        case ConvertedType::TIME_MILLIS:
        case ConvertedType::TIME_MICROS:
        case ConvertedType::INTERVAL:
        case ConvertedType::JSON:
        case ConvertedType::MAP:
        case ConvertedType::MAP_KEY_VALUE:
        case ConvertedType::LIST:
        case ConvertedType::BSON:
        case ConvertedType::UINT_8:
        case ConvertedType::UINT_16:
        case ConvertedType::UINT_32:
        case ConvertedType::UINT_64:
        case ConvertedType::TIMESTAMP_MICROS:
        case ConvertedType::TIMESTAMP_MILLIS:
        case ConvertedType::DECIMAL:
        default:
            throw common::CopyException{"Unsupported converted type"};
        }
    } else {
        // no converted type set
        // use default type for each physical type
        switch (s_ele.type) {
        case Type::BOOLEAN:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::BOOL);
        case Type::INT32:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT32);
        case Type::INT64:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT64);
        case Type::FLOAT:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::FLOAT);
        case Type::DOUBLE:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::DOUBLE);
        case Type::BYTE_ARRAY:
        case Type::FIXED_LEN_BYTE_ARRAY:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::STRING);
        default:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::ANY);
        }
    }
}

uint64_t ParquetReader::getGroupCompressedSize(ParquetReaderScanState& state) {
    auto& group = getGroup(state);
    auto total_compressed_size = group.total_compressed_size;

    uint64_t calc_compressed_size = 0;

    // If the global total_compressed_size is not set, we can still calculate it
    if (group.total_compressed_size == 0) {
        for (auto& column_chunk : group.columns) {
            calc_compressed_size += column_chunk.meta_data.total_compressed_size;
        }
    }

    if (total_compressed_size != 0 && calc_compressed_size != 0 &&
        (uint64_t)total_compressed_size != calc_compressed_size) {
        throw common::CopyException(
            "mismatch between calculated compressed size and reported compressed size");
    }

    return total_compressed_size ? total_compressed_size : calc_compressed_size;
}

uint64_t ParquetReader::getGroupOffset(ParquetReaderScanState& state) {
    auto& group = getGroup(state);
    uint64_t minOffset = UINT64_MAX;

    for (auto& column_chunk : group.columns) {
        if (column_chunk.meta_data.__isset.dictionary_page_offset) {
            minOffset =
                std::min<uint64_t>(minOffset, column_chunk.meta_data.dictionary_page_offset);
        }
        if (column_chunk.meta_data.__isset.index_page_offset) {
            minOffset = std::min<uint64_t>(minOffset, column_chunk.meta_data.index_page_offset);
        }
        minOffset = std::min<uint64_t>(minOffset, column_chunk.meta_data.data_page_offset);
    }

    return minOffset;
}

} // namespace processor
} // namespace kuzu
