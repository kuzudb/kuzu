#include "src/loader/include/in_mem_structure/builder/in_mem_structures_builder.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace loader {

InMemStructuresBuilder::InMemStructuresBuilder(label_t label, string labelName,
    string inputFilePath, string outputDirectory, CSVReaderConfig csvReaderConfig,
    TaskScheduler& taskScheduler, Catalog& catalog, LoaderProgressBar* progressBar)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")}, label{label}, labelName{move(labelName)},
      inputFilePath{move(inputFilePath)}, outputDirectory{move(outputDirectory)}, numBlocks{0},
      csvReaderConfig{csvReaderConfig}, taskScheduler{taskScheduler}, catalog{catalog},
      progressBar{progressBar} {}

uint64_t InMemStructuresBuilder::parseCSVHeaderAndCalcNumBlocks(
    const string& filePath, vector<PropertyNameDataType>& colDefinitions) {
    logger->info("Parsing csv headers and calculating number of blocks for label {}.", labelName);
    ifstream inf(filePath, ios_base::in);
    if (!inf.is_open()) {
        throw LoaderException("Cannot open file " + filePath + ".");
    }
    string fileHeader;
    do {
        getline(inf, fileHeader);
    } while (fileHeader.empty() || fileHeader.at(0) == LoaderConfig::COMMENT_LINE_CHAR);
    inf.seekg(0, ios_base::end);
    colDefinitions = parseCSVFileHeader(fileHeader);
    logger->info(
        "Done parsing csv headers and calculating number of blocks for label {}.", labelName);
    auto numBlocksInFile = 1 + (inf.tellg() / LoaderConfig::CSV_READING_BLOCK_SIZE);
    inf.close();
    return numBlocksInFile;
}

vector<PropertyNameDataType> InMemStructuresBuilder::parseCSVFileHeader(string& header) const {
    auto colHeaders = StringUtils::split(header, string(1, csvReaderConfig.tokenSeparator));
    unordered_set<string> columnNameSet;
    vector<PropertyNameDataType> colHeaderDefinitions;
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
        colHeaderDefinitions.push_back(colHeaderDefinition);
    }
    return colHeaderDefinitions;
}

// Lists headers are created for only AdjLists and UnstrPropertyLists, both of which store data
// in the page without NULL bits.
void InMemStructuresBuilder::calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
    atomic_uint64_vec_t* listSizes, ListHeaders* listHeaders,
    const shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar) {
    logger->trace("Start: ListHeaders={0:p}", (void*)listHeaders);
    auto numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, false /* hasNull */);
    auto numChunks = numNodes >> StorageConfig::LISTS_CHUNK_SIZE_LOG_2;
    if (0 != (numNodes & (StorageConfig::LISTS_CHUNK_SIZE - 1))) {
        numChunks++;
    }
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
            listHeaders->setHeader(nodeOffset, header);
            nodeOffset++;
        }
    }
    progressBar->incrementTaskFinished();
    logger->trace("End: adjListHeaders={0:p}", (void*)listHeaders);
}

void InMemStructuresBuilder::calculateListsMetadataTask(uint64_t numNodes, uint32_t elementSize,
    atomic_uint64_vec_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
    bool hasNULLBytes, const shared_ptr<spdlog::logger>& logger, LoaderProgressBar* progressBar) {
    logger->trace("Start: listsMetadata={0:p} adjListHeaders={1:p}", (void*)listsMetadata,
        (void*)listHeaders);
    auto globalPageId = 0u;
    auto numChunks = numNodes >> StorageConfig::LISTS_CHUNK_SIZE_LOG_2;
    if (0 != (numNodes & (StorageConfig::LISTS_CHUNK_SIZE - 1))) {
        numChunks++;
    }
    listsMetadata->initChunkPageLists(numChunks);
    node_offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto lastNodeOffsetInChunk = min(nodeOffset + StorageConfig::LISTS_CHUNK_SIZE, numNodes);
        for (auto i = nodeOffset; i < lastNodeOffsetInChunk; i++) {
            if (ListHeaders::isALargeList(listHeaders->getHeader(nodeOffset))) {
                largeListIdx++;
            }
            nodeOffset++;
        }
    }
    listsMetadata->initLargeListPageLists(largeListIdx);
    nodeOffset = 0u;
    largeListIdx = 0u;
    auto numPerPage = PageUtils::getNumElementsInAPage(elementSize, hasNULLBytes);
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk = min(nodeOffset + StorageConfig::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numElementsInList = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            if (ListHeaders::isALargeList(listHeaders->getHeader(nodeOffset))) {
                auto numPagesForLargeList = numElementsInList / numPerPage;
                if (0 != numElementsInList % numPerPage) {
                    numPagesForLargeList++;
                }
                listsMetadata->populateLargeListPageList(
                    largeListIdx, numPagesForLargeList, numElementsInList, globalPageId);
                globalPageId += numPagesForLargeList;
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
        if (0 == offsetInPage) {
            listsMetadata->populateChunkPageList(chunkId, numPages, globalPageId);
            globalPageId += numPages;
        } else {
            listsMetadata->populateChunkPageList(chunkId, numPages + 1, globalPageId);
            globalPageId += numPages + 1;
        }
    }
    listsMetadata->setNumPages(globalPageId);
    progressBar->incrementTaskFinished();
    logger->trace(
        "End: listsMetadata={0:p} listHeaders={1:p}", (void*)listsMetadata, (void*)listHeaders);
}

} // namespace loader
} // namespace graphflow
