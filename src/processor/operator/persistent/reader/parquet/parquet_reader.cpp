#include "processor/operator/persistent/reader/parquet/parquet_reader.h"

#include "common/exception/copy.h"
#include "common/file_utils.h"
#include "common/string_format.h"
#include "processor/operator/persistent/reader/parquet/list_column_reader.h"
#include "processor/operator/persistent/reader/parquet/struct_column_reader.h"
#include "processor/operator/persistent/reader/parquet/thrift_tools.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

using namespace kuzu_parquet::format;

namespace kuzu {
namespace processor {

using namespace kuzu::function;
using namespace kuzu::common;

ParquetReader::ParquetReader(const std::string& filePath, storage::MemoryManager* memoryManager)
    : filePath{filePath}, memoryManager{memoryManager} {
    fileInfo = FileUtils::openFile(filePath, O_RDONLY);
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
        state.fileInfo = FileUtils::openFile(fileInfo->path, O_RDONLY);
    }

    state.thriftFileProto = createThriftProtocol(state.fileInfo.get(), state.prefetchMode);
    state.rootReader = createReader();
    state.defineBuf.resize(DEFAULT_VECTOR_CAPACITY);
    state.repeatBuf.resize(DEFAULT_VECTOR_CAPACITY);
}

bool ParquetReader::scanInternal(ParquetReaderScanState& state, DataChunk& result) {
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

            // LCOV_EXCL_START
            if (toScanCompressedBytes > totalRowGroupSpan) {
                throw CopyException("Malformed parquet file: sum of total compressed bytes "
                                    "of columns seems incorrect");
            }
            // LCOV_EXCL_STOP

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
                // Prefetch column-wise.
                for (auto colIdx = 0u; colIdx < result.getNumValueVectors(); colIdx++) {
                    auto fileColIdx = colIdx;
                    auto rootReader = reinterpret_cast<StructColumnReader*>(state.rootReader.get());

                    rootReader->getChildReader(fileColIdx)
                        ->registerPrefetch(trans, true /* lazy fetch */);
                }
                trans.FinalizeRegistration();
                trans.PrefetchRegistered();
            }
        }
        return true;
    }

    auto thisOutputChunkRows =
        std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, getGroup(state).num_rows - state.groupOffset);
    result.state->selVector->selectedSize = thisOutputChunkRows;

    if (thisOutputChunkRows == 0) {
        state.finished = true;
        return false; // end of last group, we are done
    }

    // we evaluate simple table filters directly in this scan, so we can skip decoding column data
    // that's never going to be relevant
    parquet_filter_t filterMask;
    filterMask.set();

    // mask out unused part of bitset
    for (auto i = thisOutputChunkRows; i < DEFAULT_VECTOR_CAPACITY; i++) {
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
        // LCOV_EXCL_START
        if (rowsRead != result.state->selVector->selectedSize) {
            throw CopyException(
                stringFormat("Mismatch in parquet read for column {}, expected {} rows, got {}",
                    fileColIdx, result.state->selVector->selectedSize, rowsRead));
        }
        // LCOV_EXCL_STOP
    }

    state.groupOffset += thisOutputChunkRows;
    return true;
}

void ParquetReader::scan(processor::ParquetReaderScanState& state, DataChunk& result) {
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
    // LCOV_EXCL_START
    if (fileSize < 12) {
        throw CopyException{
            stringFormat("File {} is too small to be a Parquet file", fileInfo->path.c_str())};
    }
    // LCOV_EXCL_STOP

    ResizeableBuffer buf;
    buf.resize(8);
    buf.zero();

    transport.SetLocation(fileSize - 8);
    transport.read((uint8_t*)buf.ptr, 8);

    // LCOV_EXCL_START
    if (memcmp(buf.ptr + 4, "PAR1", 4) != 0) {
        if (memcmp(buf.ptr + 4, "PARE", 4) == 0) {
            throw CopyException{stringFormat(
                "Encrypted Parquet files are not supported for file {}", fileInfo->path.c_str())};
        }
        throw CopyException{
            stringFormat("No magic bytes found at the end of file {}", fileInfo->path.c_str())};
    }
    // LCOV_EXCL_STOP
    // Read four-byte footer length from just before the end magic bytes.
    auto footerLen = *reinterpret_cast<uint32_t*>(buf.ptr);
    // LCOV_EXCL_START
    if (footerLen == 0 || fileSize < 12 + footerLen) {
        throw CopyException{stringFormat("Footer length error in file {}", fileInfo->path.c_str())};
    }
    // LCOV_EXCL_STOP
    auto metadataPos = fileSize - (footerLen + 8);
    transport.SetLocation(metadataPos);
    transport.Prefetch(metadataPos, footerLen);

    metadata = std::make_unique<FileMetaData>();
    metadata->read(proto.get());
}

std::unique_ptr<ColumnReader> ParquetReader::createReaderRecursive(uint64_t depth,
    uint64_t maxDefine, uint64_t maxRepeat, uint64_t& nextSchemaIdx, uint64_t& nextFileIdx) {
    KU_ASSERT(nextSchemaIdx < metadata->schema.size());
    auto& sEle = metadata->schema[nextSchemaIdx];
    auto thisIdx = nextSchemaIdx;

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
    if (sEle.__isset.num_children && sEle.num_children > 0) {
        std::vector<std::unique_ptr<StructField>> structFields;
        std::vector<std::unique_ptr<ColumnReader>> childrenReaders;
        uint64_t cIdx = 0;
        while (cIdx < (uint64_t)sEle.num_children) {
            nextSchemaIdx++;
            auto& childEle = metadata->schema[nextSchemaIdx];
            auto childReader =
                createReaderRecursive(depth + 1, maxDefine, maxRepeat, nextSchemaIdx, nextFileIdx);
            structFields.push_back(
                std::make_unique<StructField>(childEle.name, childReader->getDataType()->copy()));
            childrenReaders.push_back(std::move(childReader));
            cIdx++;
        }
        KU_ASSERT(!structFields.empty());
        std::unique_ptr<ColumnReader> result;
        std::unique_ptr<LogicalType> resultType;

        bool isRepeated = repetition_type == FieldRepetitionType::REPEATED;
        bool isList = sEle.__isset.converted_type && sEle.converted_type == ConvertedType::LIST;
        bool isMap = sEle.__isset.converted_type && sEle.converted_type == ConvertedType::MAP;
        bool isMapKV =
            sEle.__isset.converted_type && sEle.converted_type == ConvertedType::MAP_KEY_VALUE;
        if (!isMapKV && thisIdx > 0) {
            // check if the parent node of this is a map
            auto& parentEle = metadata->schema[thisIdx - 1];
            bool parentIsMap =
                parentEle.__isset.converted_type && parentEle.converted_type == ConvertedType::MAP;
            bool parentHasChildren = parentEle.__isset.num_children && parentEle.num_children == 1;
            isMapKV = parentIsMap && parentHasChildren;
        }

        if (isMapKV) {
            // LCOV_EXCL_START
            if (structFields.size() != 2) {
                throw CopyException{"MAP_KEY_VALUE requires two children"};
            }
            if (!isRepeated) {
                throw CopyException{"MAP_KEY_VALUE needs to be repeated"};
            }
            // LCOV_EXCL_STOP
            auto structType = std::make_unique<LogicalType>(
                LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(structFields)));
            resultType = std::make_unique<LogicalType>(
                LogicalTypeID::MAP, std::make_unique<VarListTypeInfo>(std::move(structType)));

            auto structReader = std::make_unique<StructColumnReader>(*this,
                VarListType::getChildType(resultType.get())->copy(), sEle, thisIdx, maxDefine - 1,
                maxRepeat - 1, std::move(childrenReaders));
            return std::make_unique<ListColumnReader>(*this, std::move(resultType), sEle, thisIdx,
                maxDefine, maxRepeat, std::move(structReader), memoryManager);
        }

        if (structFields.size() > 1 || (!isList && !isMap && !isRepeated)) {
            resultType = std::make_unique<LogicalType>(
                LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(structFields)));
            result = std::make_unique<StructColumnReader>(*this, std::move(resultType), sEle,
                thisIdx, maxDefine, maxRepeat, std::move(childrenReaders));
        } else {
            // if we have a struct with only a single type, pull up
            resultType = structFields[0]->getType()->copy();
            result = std::move(childrenReaders[0]);
        }
        if (isRepeated) {
            auto varListInfo = std::make_unique<VarListTypeInfo>(std::move(resultType));
            resultType =
                std::make_unique<LogicalType>(LogicalTypeID::VAR_LIST, std::move(varListInfo));
            return std::make_unique<ListColumnReader>(*this, std::move(resultType), sEle, thisIdx,
                maxDefine, maxRepeat, std::move(result), memoryManager);
        }
        return result;
    } else {
        // LCOV_EXCL_START
        if (!sEle.__isset.type) {
            throw CopyException{"Node has neither num_children nor type set - this "
                                "violates the Parquet spec (corrupted file)"};
        }
        // LCOV_EXCL_STOP
        if (sEle.repetition_type == FieldRepetitionType::REPEATED) {
            auto derivedType = deriveLogicalType(sEle);
            auto varListTypeInfo = std::make_unique<VarListTypeInfo>(derivedType->copy());
            auto listType =
                std::make_unique<LogicalType>(LogicalTypeID::VAR_LIST, std::move(varListTypeInfo));
            auto elementReader = ColumnReader::createReader(
                *this, std::move(derivedType), sEle, nextFileIdx++, maxDefine, maxRepeat);
            return std::make_unique<ListColumnReader>(*this, std::move(listType), sEle, thisIdx,
                maxDefine, maxRepeat, std::move(elementReader), memoryManager);
        }
        // TODO check return value of derive type or should we only do this on read()
        return ColumnReader::createReader(
            *this, deriveLogicalType(sEle), sEle, nextFileIdx++, maxDefine, maxRepeat);
    }
}

std::unique_ptr<ColumnReader> ParquetReader::createReader() {
    uint64_t nextSchemaIdx = 0;
    uint64_t nextFileIdx = 0;

    // LCOV_EXCL_START
    if (metadata->schema.empty()) {
        throw CopyException{"Parquet reader: no schema elements found"};
    }
    if (metadata->schema[0].num_children == 0) {
        throw CopyException{"Parquet reader: root schema element has no children"};
    }
    // LCOV_EXCL_STOP
    auto rootReader = createReaderRecursive(0, 0, 0, nextSchemaIdx, nextFileIdx);
    // LCOV_EXCL_START
    if (rootReader->getDataType()->getPhysicalType() != PhysicalTypeID::STRUCT) {
        throw CopyException{"Root element of Parquet file must be a struct"};
    }
    // LCOV_EXCL_STOP
    for (auto& field : StructType::getFields(rootReader->getDataType())) {
        columnNames.push_back(field->getName());
        columnTypes.push_back(field->getType()->copy());
    }

    KU_ASSERT(nextSchemaIdx == metadata->schema.size() - 1);
    KU_ASSERT(
        metadata->row_groups.empty() || nextFileIdx == metadata->row_groups[0].columns.size());
    return rootReader;
}

void ParquetReader::prepareRowGroupBuffer(ParquetReaderScanState& state, uint64_t /*colIdx*/) {
    auto& group = getGroup(state);
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

std::unique_ptr<LogicalType> ParquetReader::deriveLogicalType(
    const kuzu_parquet::format::SchemaElement& s_ele) {
    // inner node
    if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
        // LCOV_EXCL_START
        throw CopyException("FIXED_LEN_BYTE_ARRAY requires length to be set");
        // LCOV_EXCL_STOP
    }
    if (s_ele.__isset.converted_type) {
        switch (s_ele.converted_type) {
        case ConvertedType::INT_8:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::INT8);
            } else {
                // LCOV_EXCL_START
                throw CopyException{"INT8 converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::INT_16:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::INT16);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "INT16 converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::INT_32:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::INT32);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "INT32 converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::INT_64:
            if (s_ele.type == Type::INT64) {
                return std::make_unique<LogicalType>(LogicalTypeID::INT64);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "INT64 converted type can only be set for value of Type::INT64"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::UINT_8:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::UINT8);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "UINT8 converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::UINT_16:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::UINT16);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "UINT16 converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::UINT_32:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::UINT32);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "UINT32 converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::UINT_64:
            if (s_ele.type == Type::INT64) {
                return std::make_unique<LogicalType>(LogicalTypeID::UINT64);
            } else {
                // LCOV_EXCL_START
                throw CopyException{
                    "UINT64 converted type can only be set for value of Type::INT64"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::DATE:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<LogicalType>(LogicalTypeID::DATE);
            } else {
                // LCOV_EXCL_START
                throw CopyException{"DATE converted type can only be set for value of Type::INT32"};
                // LCOV_EXCL_STOP
            }
        case ConvertedType::TIMESTAMP_MICROS:
        case ConvertedType::TIMESTAMP_MILLIS:
            if (s_ele.type == Type::INT64) {
                return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP);
            } else {
                // LCOV_EXCL_START
                throw CopyException(
                    "TIMESTAMP converted type can only be set for value of Type::INT64");
                // LCOV_EXCL_STOP
            }
        case ConvertedType::INTERVAL: {
            return std::make_unique<LogicalType>(LogicalTypeID::INTERVAL);
        }
        case ConvertedType::UTF8:
            switch (s_ele.type) {
            case Type::BYTE_ARRAY:
            case Type::FIXED_LEN_BYTE_ARRAY:
                return std::make_unique<LogicalType>(LogicalTypeID::STRING);
                // LCOV_EXCL_START
            default:
                throw CopyException(
                    "UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
                // LCOV_EXCL_STOP
            }
        default:
            // LCOV_EXCL_START
            throw CopyException{"Unsupported converted type"};
            // LCOV_EXCL_STOP
        }
    } else {
        // no converted type set
        // use default type for each physical type
        switch (s_ele.type) {
        case Type::BOOLEAN:
            return std::make_unique<LogicalType>(LogicalTypeID::BOOL);
        case Type::INT32:
            return std::make_unique<LogicalType>(LogicalTypeID::INT32);
        case Type::INT64:
            return std::make_unique<LogicalType>(LogicalTypeID::INT64);
        case Type::INT96:
            return std::make_unique<LogicalType>(LogicalTypeID::TIMESTAMP);
        case Type::FLOAT:
            return std::make_unique<LogicalType>(LogicalTypeID::FLOAT);
        case Type::DOUBLE:
            return std::make_unique<LogicalType>(LogicalTypeID::DOUBLE);
        case Type::BYTE_ARRAY:
        case Type::FIXED_LEN_BYTE_ARRAY:
            // TODO(Ziyi): Support parquet copy option(binary_as_string).
            return std::make_unique<LogicalType>(LogicalTypeID::BLOB);
        default:
            return std::make_unique<LogicalType>(LogicalTypeID::ANY);
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

    // LCOV_EXCL_START
    if (total_compressed_size != 0 && calc_compressed_size != 0 &&
        (uint64_t)total_compressed_size != calc_compressed_size) {
        throw CopyException(
            "mismatch between calculated compressed size and reported compressed size");
    }
    // LCOV_EXCL_STOP

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

ParquetScanSharedState::ParquetScanSharedState(const common::ReaderConfig readerConfig,
    storage::MemoryManager* memoryManager, uint64_t numRows)
    : ScanSharedState{std::move(readerConfig), numRows}, memoryManager{memoryManager} {
    readers.push_back(
        std::make_unique<ParquetReader>(this->readerConfig.filePaths[fileIdx], memoryManager));
}

function_set ParquetScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(READ_PARQUET_FUNC_NAME, tableFunc, bindFunc,
            initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

bool parquetSharedStateNext(
    ParquetScanLocalState& localState, ParquetScanSharedState& sharedState) {
    std::lock_guard<std::mutex> mtx{sharedState.lock};
    while (true) {
        if (sharedState.fileIdx >= sharedState.readerConfig.getNumFiles()) {
            return false;
        }
        if (sharedState.blockIdx < sharedState.readers[sharedState.fileIdx]->getNumRowsGroups()) {
            localState.reader = sharedState.readers[sharedState.fileIdx].get();
            localState.reader->initializeScan(*localState.state, {sharedState.blockIdx});
            sharedState.blockIdx++;
            return true;
        } else {
            sharedState.blockIdx = 0;
            sharedState.fileIdx++;
            if (sharedState.fileIdx >= sharedState.readerConfig.getNumFiles()) {
                return false;
            }
            sharedState.readers.push_back(std::make_unique<ParquetReader>(
                sharedState.readerConfig.filePaths[sharedState.fileIdx],
                sharedState.memoryManager));
            continue;
        }
    }
}

void ParquetScanFunction::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    if (input.localState == nullptr) {
        return;
    }
    auto parquetScanLocalState = reinterpret_cast<ParquetScanLocalState*>(input.localState);
    auto parquetScanSharedState = reinterpret_cast<ParquetScanSharedState*>(input.sharedState);
    do {
        parquetScanLocalState->reader->scan(*parquetScanLocalState->state, outputChunk);
        if (outputChunk.state->selVector->selectedSize > 0) {
            return;
        }
        if (!parquetSharedStateNext(*parquetScanLocalState, *parquetScanSharedState)) {
            return;
        }
    } while (true);
}

std::unique_ptr<function::TableFuncBindData> ParquetScanFunction::bindFunc(
    main::ClientContext* /*context*/, function::TableFuncBindInput* input,
    catalog::CatalogContent* /*catalog*/) {
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    bindColumns(scanInput->config, scanInput->mm, detectedColumnNames, detectedColumnTypes);
    std::vector<std::string> resultColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);
    if (!scanInput->expectedColumnTypes.empty()) {
        ReaderBindUtils::validateColumnTypes(
            scanInput->expectedColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes);
    }
    return std::make_unique<function::ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->mm, scanInput->config);
}

std::unique_ptr<function::TableFuncSharedState> ParquetScanFunction::initSharedState(
    TableFunctionInitInput& input) {
    auto parquetScanBindData = reinterpret_cast<ScanBindData*>(input.bindData);
    row_idx_t numRows = 0;
    for (const auto& path : parquetScanBindData->config.filePaths) {
        auto reader = std::make_unique<ParquetReader>(path, parquetScanBindData->mm);
        numRows += reader->getMetadata()->num_rows;
    }
    return std::make_unique<ParquetScanSharedState>(
        parquetScanBindData->config, parquetScanBindData->mm, numRows);
}

std::unique_ptr<function::TableFuncLocalState> ParquetScanFunction::initLocalState(
    TableFunctionInitInput& /*input*/, TableFuncSharedState* state) {
    auto parquetScanSharedState = reinterpret_cast<ParquetScanSharedState*>(state);
    auto localState = std::make_unique<ParquetScanLocalState>();
    if (!parquetSharedStateNext(*localState, *parquetScanSharedState)) {
        return nullptr;
    }
    return localState;
}

void ParquetScanFunction::bindColumns(const common::ReaderConfig& readerConfig,
    storage::MemoryManager* mm, std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    KU_ASSERT(readerConfig.getNumFiles() > 0);
    bindColumns(readerConfig, 0, mm, columnNames, columnTypes);
    for (auto i = 1; i < readerConfig.getNumFiles(); ++i) {
        std::vector<std::string> tmpColumnNames;
        std::vector<std::unique_ptr<LogicalType>> tmpColumnTypes;
        bindColumns(readerConfig, i, mm, tmpColumnNames, tmpColumnTypes);
        ReaderBindUtils::validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
        ReaderBindUtils::validateColumnTypes(columnNames, columnTypes, tmpColumnTypes);
    }
}

void ParquetScanFunction::bindColumns(const common::ReaderConfig& readerConfig, uint32_t fileIdx,
    storage::MemoryManager* mm, std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    auto reader = ParquetReader(readerConfig.filePaths[fileIdx], mm);
    auto state = std::make_unique<processor::ParquetReaderScanState>();
    reader.initializeScan(*state, std::vector<uint64_t>{});
    for (auto i = 0u; i < reader.getNumColumns(); ++i) {
        columnNames.push_back(reader.getColumnName(i));
        columnTypes.push_back(reader.getColumnType(i)->copy());
    }
}

} // namespace processor
} // namespace kuzu
