#include "src/loader/in_mem_builder/include/in_mem_structures_builder.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace loader {

InMemStructuresBuilder::InMemStructuresBuilder(string labelName, string inputFilePath,
    string outputDirectory, CSVReaderConfig csvReaderConfig, TaskScheduler& taskScheduler,
    CatalogBuilder& catalogBuilder, LoaderProgressBar* progressBar)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")}, labelName{move(labelName)},
      inputFilePath{move(inputFilePath)}, outputDirectory{move(outputDirectory)}, numBlocks{0},
      csvReaderConfig{csvReaderConfig}, taskScheduler{taskScheduler},
      catalogBuilder{catalogBuilder}, progressBar{progressBar} {}

vector<PropertyNameDataType> InMemStructuresBuilder::parseCSVHeader(const string& filePath) {
    logger->info("Parsing csv header for label {}.", labelName);
    ifstream inf(filePath, ios_base::in);
    if (!inf.is_open()) {
        throw LoaderException("Cannot open file " + filePath + ".");
    }
    string headerLine;
    do {
        getline(inf, headerLine);
    } while (headerLine.empty() || headerLine.at(0) == LoaderConfig::COMMENT_LINE_CHAR);
    inf.close();
    logger->info("Done parsing csv header for label {}.", labelName);
    return parseHeaderLine(headerLine);
}

void InMemStructuresBuilder::calculateNumBlocks(const string& filePath) {
    logger->info("Chunking csv into blocks for label {}.", labelName);
    ifstream inf(filePath, ios_base::in);
    if (!inf.is_open()) {
        throw LoaderException("Cannot open file " + filePath + ".");
    }
    inf.seekg(0, ios_base::end);
    numBlocks = 1 + (inf.tellg() / LoaderConfig::CSV_READING_BLOCK_SIZE);
    inf.close();
    logger->info("Done chunking csv into blocks for label {}.", labelName);
}

uint64_t InMemStructuresBuilder::calculateNumRowsWithoutHeader() {
    assert(numLinesPerBlock.size() == numBlocks);
    auto numRows = 0u;
    // Decrement the header line. Note the following line should be changed if csv may not have
    // header.
    numLinesPerBlock[0]--;
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        numRows += numLinesPerBlock[blockId];
    }
    return numRows;
}

static void collectUnstrPropertyNamesInLine(
    CSVReader& reader, uint64_t numTokensToSkip, unordered_set<string>* unstrPropertyNames) {
    for (auto i = 0u; i < numTokensToSkip; ++i) {
        reader.hasNextToken();
    }
    while (reader.hasNextToken()) {
        auto unstrPropertyStr = reader.getString();
        auto unstrPropertyName =
            StringUtils::split(unstrPropertyStr, LoaderConfig::UNSTR_PROPERTY_SEPARATOR)[0];
        unstrPropertyNames->insert(unstrPropertyName);
    }
}

void InMemStructuresBuilder::countNumLinesAndUnstrPropertiesPerBlockTask(const string& fName,
    uint64_t blockId, InMemStructuresBuilder* builder, uint64_t numTokensToSkip,
    unordered_set<string>* unstrPropertyNames) {
    builder->logger->trace("Start: path=`{0}` blkIdx={1}", fName, blockId);
    CSVReader reader(fName, builder->csvReaderConfig, blockId);
    builder->numLinesPerBlock[blockId] = 0ull;
    while (reader.hasNextLine()) {
        builder->numLinesPerBlock[blockId]++;
        if (unstrPropertyNames != nullptr) {
            collectUnstrPropertyNamesInLine(reader, numTokensToSkip, unstrPropertyNames);
        }
    }
    builder->logger->trace("End: path=`{0}` blkIdx={1}", fName, blockId);
    builder->progressBar->incrementTaskFinished();
}

vector<PropertyNameDataType> InMemStructuresBuilder::parseHeaderLine(string& header) const {
    auto colHeaders = StringUtils::split(header, string(1, csvReaderConfig.tokenSeparator));
    unordered_set<string> columnNameSet;
    vector<PropertyNameDataType> propertyDefinitions;
    for (auto& colHeader : colHeaders) {
        auto colHeaderComponents =
            StringUtils::split(colHeader, LoaderConfig::PROPERTY_DATATYPE_SEPARATOR);
        if (colHeaderComponents.size() < 2) {
            throw LoaderException("Incomplete column header '" + colHeader + "'.");
        }
        PropertyNameDataType colHeaderDefinition;
        // Check each one of the mandatory field column header.
        if (colHeaderComponents[0].empty()) {
            colHeaderDefinition.name = colHeaderComponents[1];
            if (colHeaderDefinition.name == LoaderConfig::ID_FIELD) {
                // We don't explicitly have to set the ID column in PropertyNameDataType, so we only
                // skip over this property.
            } else if (colHeaderDefinition.name == LoaderConfig::START_ID_FIELD ||
                       colHeaderDefinition.name == LoaderConfig::END_ID_FIELD) {
                colHeaderDefinition.dataType.typeID = NODE_ID;
            } else if (colHeaderDefinition.name == LoaderConfig::START_ID_LABEL_FIELD ||
                       colHeaderDefinition.name == LoaderConfig::END_ID_LABEL_FIELD) {
                colHeaderDefinition.dataType.typeID = LABEL;
            } else {
                throw LoaderException(
                    "Invalid mandatory field column header '" + colHeaderDefinition.name + "'.");
            }
        } else {
            colHeaderDefinition.name = colHeaderComponents[0];
            colHeaderDefinition.dataType = Types::dataTypeFromString(colHeaderComponents[1]);
            if (colHeaderDefinition.dataType.typeID == NODE_ID ||
                colHeaderDefinition.dataType.typeID == LABEL) {
                throw LoaderException("PropertyNameDataType column header cannot be of system "
                                      "types NODE_ID or LABEL.");
            }
        }
        if (columnNameSet.find(colHeaderDefinition.name) != columnNameSet.end()) {
            throw LoaderException("Column " + colHeaderDefinition.name +
                                  " already appears previously in the column headers.");
        }
        columnNameSet.insert(colHeaderDefinition.name);
        propertyDefinitions.push_back(colHeaderDefinition);
    }
    return propertyDefinitions;
}

// Lists headers are created for only AdjLists and UnstrPropertyLists, both of which store data
// in the page without NULL bits.
void InMemStructuresBuilder::calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
    atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    const shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar) {
    logger->trace("Start: ListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
    auto numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, false /* hasNull */);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    node_offset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto lastNodeOffsetInChunk = min(nodeOffset + StorageConfig::LISTS_CHUNK_SIZE, numNodes);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(memory_order_relaxed);
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
    progressBar->incrementTaskFinished();
    logger->trace("End: adjListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
}

void InMemStructuresBuilder::calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
    uint32_t elementSize, atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    InMemLists* inMemList, bool hasNULLBytes, const shared_ptr<spdlog::logger>& logger,
    LoaderProgressBar* progressBar) {
    logger->trace("Start: listsMetadataBuilder={0:p} adjListHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    node_offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto lastNodeOffsetInChunk = min(nodeOffset + StorageConfig::LISTS_CHUNK_SIZE, numNodes);
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
        auto lastNodeOffsetInChunk = min(nodeOffset + StorageConfig::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(memory_order_relaxed);
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
    progressBar->incrementTaskFinished();
    logger->trace("End: listsMetadata={0:p} listHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
}

} // namespace loader
} // namespace graphflow
