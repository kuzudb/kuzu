#include "storage/copy_arrow/copy_structures_arrow.h"

#include "common/configs.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

CopyStructuresArrow::CopyStructuresArrow(CopyDescription& copyDescription,
    std::string outputDirectory, TaskScheduler& taskScheduler, Catalog& catalog)
    : logger{LoggerUtils::getOrCreateLogger("loader")}, copyDescription{copyDescription},
      outputDirectory{std::move(outputDirectory)}, numBlocks{0},
      taskScheduler{taskScheduler}, catalog{catalog}, numRows{0} {}

// Lists headers are created for only AdjLists, which store data in the page without NULL bits.
void CopyStructuresArrow::calculateListHeadersTask(offset_t numNodes, uint32_t elementSize,
    atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    const std::shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: ListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
    auto numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, false /* hasNull */);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    offset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConfig::LISTS_CHUNK_SIZE, numNodes);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(std::memory_order_relaxed);
            uint32_t header;
            if (numElementsInList >= numElementsPerPage) {
                header = ListHeaders::getLargeListHeader(lAdjListsIdx++);
            } else {
                header = ListHeaders::getSmallListHeader(csrOffset, numElementsInList);
                csrOffset += numElementsInList;
            }
            listHeadersBuilder->setHeader(nodeOffset, header);
            nodeOffset++;
        }
    }
    logger->trace("End: adjListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
}

void CopyStructuresArrow::calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
    uint32_t elementSize, atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    InMemLists* inMemList, bool hasNULLBytes, const std::shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: listsMetadataBuilder={0:p} adjListHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConfig::LISTS_CHUNK_SIZE, numNodes);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            if (ListHeaders::isALargeList(listHeadersBuilder->getHeader(nodeOffset))) {
                largeListIdx++;
            }
            nodeOffset++;
        }
    }
    inMemList->getListsMetadataBuilder()->initLargeListPageLists(largeListIdx);
    nodeOffset = 0u;
    largeListIdx = 0u;
    auto numPerPage = PageUtils::getNumElementsInAPage(elementSize, hasNULLBytes);
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConfig::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(std::memory_order_relaxed);
            if (ListHeaders::isALargeList(listHeadersBuilder->getHeader(nodeOffset))) {
                auto numPagesForLargeList = numElementsInList / numPerPage;
                if (0 != numElementsInList % numPerPage) {
                    numPagesForLargeList++;
                }
                inMemList->getListsMetadataBuilder()->populateLargeListPageList(largeListIdx,
                    numPagesForLargeList, numElementsInList,
                    inMemList->inMemFile->getNumPages() /* start idx of pages in .lists file */);
                inMemList->inMemFile->addNewPages(numPagesForLargeList);
                largeListIdx++;
            } else {
                while (numElementsInList + offsetInPage > numPerPage) {
                    numElementsInList -= (numPerPage - offsetInPage);
                    numPages++;
                    offsetInPage = 0;
                }
                offsetInPage += numElementsInList;
            }
            nodeOffset++;
        }
        if (0 != offsetInPage) {
            numPages++;
        }
        inMemList->getListsMetadataBuilder()->populateChunkPageList(chunkId, numPages,
            inMemList->inMemFile->getNumPages() /* start idx of pages in .lists file */);
        inMemList->inMemFile->addNewPages(numPages);
    }
    logger->trace("End: listsMetadata={0:p} listHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
}

void CopyStructuresArrow::countNumLines(const std::string& filePath) {
    arrow::Status status;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV:
        status = countNumLinesCSV(filePath);
        break;

    case CopyDescription::FileType::ARROW:
        status = countNumLinesArrow(filePath);
        break;

    case CopyDescription::FileType::PARQUET:
        status = countNumLinesParquet(filePath);
        break;
    }
    throwCopyExceptionIfNotOK(status);
}

arrow::Status CopyStructuresArrow::countNumLinesCSV(const std::string& filePath) {
    std::shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
    auto status = initCSVReaderAndCheckStatus(csv_streaming_reader, filePath);
    numRows = 0;
    numBlocks = 0;
    std::shared_ptr<arrow::RecordBatch> currBatch;

    auto endIt = csv_streaming_reader->end();
    for (auto it = csv_streaming_reader->begin(); it != endIt; ++it) {
        ARROW_ASSIGN_OR_RAISE(currBatch, *it);
        ++numBlocks;
        auto currNumRows = currBatch->num_rows();
        numLinesPerBlock.push_back(currNumRows);
        numRows += currNumRows;
    }
    return status;
}

arrow::Status CopyStructuresArrow::countNumLinesArrow(const std::string& filePath) {
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    auto status = initArrowReaderAndCheckStatus(ipc_reader, filePath);
    numRows = 0;
    numBlocks = ipc_reader->num_record_batches();
    numLinesPerBlock.resize(numBlocks);
    std::shared_ptr<arrow::RecordBatch> rbatch;

    for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
        ARROW_ASSIGN_OR_RAISE(rbatch, ipc_reader->ReadRecordBatch(blockId));
        numLinesPerBlock[blockId] = rbatch->num_rows();
        numRows += rbatch->num_rows();
    }
    return status;
}

arrow::Status CopyStructuresArrow::countNumLinesParquet(const std::string& filePath) {
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = initParquetReaderAndCheckStatus(reader, filePath);

    numRows = 0;
    numBlocks = reader->num_row_groups();
    numLinesPerBlock.resize(numBlocks);
    std::shared_ptr<arrow::Table> table;

    for (uint64_t blockId = 0; blockId < numBlocks; ++blockId) {
        ARROW_RETURN_NOT_OK(reader->RowGroup(blockId)->ReadTable(&table));
        numLinesPerBlock[blockId] = table->num_rows();
        numRows += table->num_rows();
    }
    return status;
}

arrow::Status CopyStructuresArrow::initCSVReaderAndCheckStatus(
    std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
    const std::string& filePath) {
    auto status = initCSVReader(csv_streaming_reader, filePath);
    throwCopyExceptionIfNotOK(status);
    return status;
}

arrow::Status CopyStructuresArrow::initCSVReader(
    std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
    const std::string& filePath) {
    std::shared_ptr<arrow::io::InputStream> arrow_input_stream;
    ARROW_ASSIGN_OR_RAISE(arrow_input_stream, arrow::io::ReadableFile::Open(filePath));
    auto arrowRead = arrow::csv::ReadOptions::Defaults();
    arrowRead.block_size = CopyConfig::CSV_READING_BLOCK_SIZE;

    if (!copyDescription.csvReaderConfig->hasHeader) {
        arrowRead.autogenerate_column_names = true;
    }

    auto arrowConvert = arrow::csv::ConvertOptions::Defaults();
    arrowConvert.strings_can_be_null = true;
    arrowConvert.quoted_strings_can_be_null = false;

    auto arrowParse = arrow::csv::ParseOptions::Defaults();
    arrowParse.delimiter = copyDescription.csvReaderConfig->delimiter;
    arrowParse.escape_char = copyDescription.csvReaderConfig->escapeChar;
    arrowParse.quote_char = copyDescription.csvReaderConfig->quoteChar;
    arrowParse.escaping = true;

    ARROW_ASSIGN_OR_RAISE(
        csv_streaming_reader, arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                  arrow_input_stream, arrowRead, arrowParse, arrowConvert));
    return arrow::Status::OK();
}

arrow::Status CopyStructuresArrow::initArrowReaderAndCheckStatus(
    std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader, const std::string& filePath) {
    auto status = initArrowReader(ipc_reader, filePath);
    throwCopyExceptionIfNotOK(status);
    return status;
}

arrow::Status CopyStructuresArrow::initArrowReader(
    std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader, const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;

    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));

    ARROW_ASSIGN_OR_RAISE(ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));
    return arrow::Status::OK();
}

arrow::Status CopyStructuresArrow::initParquetReaderAndCheckStatus(
    std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath) {
    auto status = initParquetReader(reader, filePath);
    throwCopyExceptionIfNotOK(status);
    return status;
}

arrow::Status CopyStructuresArrow::initParquetReader(
    std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(
        infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));

    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    return arrow::Status::OK();
}

std::unique_ptr<Value> CopyStructuresArrow::getArrowList(std::string& l, int64_t from, int64_t to,
    const DataType& dataType, CopyDescription& copyDescription) {
    auto childDataType = *dataType.childType;

    char delimiter = copyDescription.csvReaderConfig->delimiter;
    std::vector<std::pair<int64_t, int64_t>> split;
    int bracket = 0;
    int64_t last = from;
    if (dataType.typeID == LIST) {
        for (int64_t i = from; i <= to; i++) {
            if (l[i] == copyDescription.csvReaderConfig->listBeginChar) {
                bracket += 1;
            } else if (l[i] == copyDescription.csvReaderConfig->listEndChar) {
                bracket -= 1;
            } else if (bracket == 0 && l[i] == delimiter) {
                split.emplace_back(last, i - last);
                last = i + 1;
            }
        }
    }
    split.emplace_back(last, to - last + 1);

    std::vector<std::unique_ptr<Value>> values;
    for (auto pair : split) {
        std::string element = l.substr(pair.first, pair.second);
        if (element.empty()) {
            continue;
        }
        std::unique_ptr<Value> value;
        switch (childDataType.typeID) {
        case INT64: {
            value = std::make_unique<Value>((int64_t)stoll(element));
        } break;
        case DOUBLE: {
            value = std::make_unique<Value>(stod(element));
        } break;
        case BOOL: {
            transform(element.begin(), element.end(), element.begin(), ::tolower);
            std::istringstream is(element);
            bool b;
            is >> std::boolalpha >> b;
            value = std::make_unique<Value>(b);
        } break;
        case STRING: {
            value = make_unique<Value>(element);
        } break;
        case DATE: {
            value = std::make_unique<Value>(Date::FromCString(element.c_str(), element.length()));
        } break;
        case TIMESTAMP: {
            value =
                std::make_unique<Value>(Timestamp::FromCString(element.c_str(), element.length()));
        } break;
        case INTERVAL: {
            value =
                std::make_unique<Value>(Interval::FromCString(element.c_str(), element.length()));
        } break;
        case LIST: {
            value = getArrowList(l, pair.first + 1, pair.second + pair.first - 1,
                *dataType.childType, copyDescription);
        } break;
        default:
            throw ReaderException("Unsupported data type " +
                                  Types::dataTypeToString(dataType.childType->typeID) +
                                  " inside LIST");
        }
        values.push_back(std::move(value));
    }
    auto numBytesOfOverflow = values.size() * Types::getDataTypeSize(dataType.typeID);
    if (numBytesOfOverflow >= DEFAULT_PAGE_SIZE) {
        throw ReaderException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.", DEFAULT_PAGE_SIZE,
            numBytesOfOverflow));
    }
    return make_unique<Value>(
        DataType(LIST, std::make_unique<DataType>(childDataType)), std::move(values));
}

void CopyStructuresArrow::throwCopyExceptionIfNotOK(const arrow::Status& status) {
    if (!status.ok()) {
        throw CopyException(status.ToString());
    }
}

} // namespace storage
} // namespace kuzu
