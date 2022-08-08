#include "src/processor/include/physical_plan/operator/copy_csv/in_mem_builder/in_mem_structures_builder.h"

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace processor {

InMemStructuresBuilder::InMemStructuresBuilder(const string inputFilePath,
    const CSVReaderConfig csvReaderConfig, string outputDirectory, TaskScheduler& taskScheduler,
    const Catalog& catalog)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")}, inputFilePath{move(inputFilePath)},
      csvReaderConfig{move(csvReaderConfig)}, outputDirectory{move(outputDirectory)}, numBlocks{0},
      taskScheduler{taskScheduler}, catalog{catalog} {}

void InMemStructuresBuilder::calculateNumBlocks(const string& filePath, string labelName) {
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

uint64_t InMemStructuresBuilder::calculateNumRows() {
    assert(numLinesPerBlock.size() == numBlocks);
    auto numRows = 0u;
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
}

// Lists headers are created for only AdjLists and UnstrPropertyLists, both of which store data
// in the page without NULL bits.
void InMemStructuresBuilder::calculateListHeadersTask(node_offset_t numNodes, uint32_t elementSize,
    atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
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

void InMemStructuresBuilder::calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
    uint32_t elementSize, atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
    InMemLists* inMemList, bool hasNULLBytes, const shared_ptr<spdlog::logger>& logger) {
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

} // namespace processor
} // namespace graphflow
