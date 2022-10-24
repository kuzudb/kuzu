#include "include/in_mem_structures_csv_copier.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

InMemStructuresCSVCopier::InMemStructuresCSVCopier(CSVDescription& csvDescription,
    string outputDirectory, TaskScheduler& taskScheduler, Catalog& catalog)
    : logger{LoggerUtils::getOrCreateSpdLogger("CopyCSV")}, csvDescription{csvDescription},
      outputDirectory{move(outputDirectory)}, numBlocks{0},
      taskScheduler{taskScheduler}, catalog{catalog} {}

void InMemStructuresCSVCopier::calculateNumBlocks(const string& filePath, string tableName) {
    logger->info("Chunking csv into blocks for table {}.", tableName);
    ifstream inf(filePath, ios_base::in);
    if (!inf.is_open()) {
        throw CopyCSVException("Cannot open file " + filePath + ".");
    }
    inf.seekg(0, ios_base::end);
    numBlocks = 1 + (inf.tellg() / CopyCSVConfig::CSV_READING_BLOCK_SIZE);
    inf.close();
    logger->info("Done chunking csv into blocks for table {}.", tableName);
}

uint64_t InMemStructuresCSVCopier::calculateNumRows(bool hasHeader) {
    assert(numLinesPerBlock.size() == numBlocks);
    auto numRows = 0u;
    if (hasHeader) {
        // Decrement the header line.
        numLinesPerBlock[0]--;
    }
    for (auto blockId = 0u; blockId < numBlocks; blockId++) {
        numRows += numLinesPerBlock[blockId];
    }
    return numRows;
}

static void collectUnstrPropertyNamesInLine(
    CSVReader& reader, uint64_t numTokensToSkip, unordered_map<string, DataType>* adhocProperties) {
    for (auto i = 0u; i < numTokensToSkip; ++i) {
        reader.hasNextToken();
    }
    while (reader.hasNextToken()) {
        auto adhocPropertyStr = reader.getString();
        auto splittedAdhocPropertyStr =
            StringUtils::split(adhocPropertyStr, CopyCSVConfig::UNSTR_PROPERTY_SEPARATOR);
        auto propertyName = splittedAdhocPropertyStr[0];
        auto dataType = Types::dataTypeFromString(splittedAdhocPropertyStr[1]);
        if (adhocProperties->contains(propertyName)) {
            // TODO(Chang): throw warning
            assert(adhocProperties->at(propertyName) == dataType);
        }
        adhocProperties->insert({propertyName, dataType});
    }
}

void InMemStructuresCSVCopier::countNumLinesAndAdhocPropertiesPerBlockTask(const string& fName,
    uint64_t blockId, InMemStructuresCSVCopier* copier, uint64_t numTokensToSkip,
    unordered_map<string, DataType>* adhocProperties) {
    copier->logger->trace("Start: path=`{0}` blkIdx={1}", fName, blockId);
    CSVReader reader(fName, copier->csvDescription.csvReaderConfig, blockId);
    copier->numLinesPerBlock[blockId] = 0ull;
    while (reader.hasNextLine()) {
        copier->numLinesPerBlock[blockId]++;
        if (adhocProperties != nullptr) {
            collectUnstrPropertyNamesInLine(reader, numTokensToSkip, adhocProperties);
        }
    }
    copier->logger->trace("End: path=`{0}` blkIdx={1}", fName, blockId);
}

// Lists headers are created for only AdjLists and UnstrPropertyLists, both of which store data
// in the page without NULL bits.
void InMemStructuresCSVCopier::calculateListHeadersTask(node_offset_t numNodes,
    uint32_t elementSize, atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    const shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: ListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
    auto numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, false /* hasNull */);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    node_offset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto lastNodeOffsetInChunk =
            min(nodeOffset + ListsMetadataConfig::LISTS_CHUNK_SIZE, numNodes);
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
    logger->trace("End: adjListHeadersBuilder={0:p}", (void*)listHeadersBuilder);
}

void InMemStructuresCSVCopier::calculateListsMetadataAndAllocateInMemListPagesTask(
    uint64_t numNodes, uint32_t elementSize, atomic_uint64_vec_t* listSizes,
    ListHeadersBuilder* listHeadersBuilder, InMemLists* inMemList, bool hasNULLBytes,
    const shared_ptr<spdlog::logger>& logger) {
    logger->trace("Start: listsMetadataBuilder={0:p} adjListHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    node_offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto lastNodeOffsetInChunk =
            min(nodeOffset + ListsMetadataConfig::LISTS_CHUNK_SIZE, numNodes);
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
            min(nodeOffset + ListsMetadataConfig::LISTS_CHUNK_SIZE, numNodes);
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
    logger->trace("End: listsMetadata={0:p} listHeadersBuilder={1:p}",
        (void*)inMemList->getListsMetadataBuilder(), (void*)listHeadersBuilder);
}

} // namespace storage
} // namespace graphflow
