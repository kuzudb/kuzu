#include "src/common/loader/include/rels_loader.h"

#include "src/common/loader/include/csv_reader.h"
#include "src/storage/include/indexes.h"

namespace graphflow {
namespace common {

void RelsLoader::load(vector<string> &fnames, vector<uint64_t> &numBlocksPerLabel) {
    RelLabelMetadata relLabelMetadata;
    for (auto relLabel = 0u; relLabel < catalog.getRelLabelsCount(); relLabel++) {
        relLabelMetadata.label = relLabel;
        relLabelMetadata.fname = fnames[relLabel];
        relLabelMetadata.numBlocks = numBlocksPerLabel[relLabel];
        for (auto dir : DIRECTIONS) {
            relLabelMetadata.isSingleCardinalityPerDir[dir] =
                catalog.isSingleCaridinalityInDir(relLabelMetadata.label, dir);
            relLabelMetadata.nodeLabelsPerDir[dir] =
                catalog.getNodeLabelsForRelLabelDir(relLabelMetadata.label, dir);
        }
        for (auto dir : DIRECTIONS) {
            relLabelMetadata.numBytesSchemePerDir[dir] =
                Indexes::getNumBytesScheme(relLabelMetadata.nodeLabelsPerDir[!dir],
                    graph.getNumNodesPerLabel(), catalog.getNodeLabelsCount());
        }
        loadRelsForLabel(relLabelMetadata);
    }
}

void RelsLoader::loadRelsForLabel(RelLabelMetadata &relLabelMetadata) {
    logger->info("Creating adjLists for rel label {}.", relLabelMetadata.label);
    dirLabelListSizes_t dirLabelListSizes{2};
    dirLabelAdjListsMetadata_t dirLabelAdjListsMetadata{2};
    for (auto dir : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[dir]) {
            dirLabelAdjListsMetadata[dir].resize(catalog.getNodeLabelsCount());
            dirLabelListSizes[dir].resize(catalog.getNodeLabelsCount());
            for (auto nodeLabel : relLabelMetadata.nodeLabelsPerDir[dir]) {
                dirLabelListSizes[dir][nodeLabel] =
                    make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesForLabel(nodeLabel));
            }
        }
    }
    constructAdjEdgesAndCountRelsInAdjLists(relLabelMetadata, dirLabelListSizes);
    if (!relLabelMetadata.isSingleCardinalityPerDir[FORWARD] ||
        !relLabelMetadata.isSingleCardinalityPerDir[BACKWARD]) {
        contructAdjLists(relLabelMetadata, dirLabelListSizes, dirLabelAdjListsMetadata);
    }
    logger->info("done.");
}

void RelsLoader::constructAdjEdgesAndCountRelsInAdjLists(
    RelLabelMetadata &relLabelMetadata, dirLabelListSizes_t &dirLabelListSizes) {
    auto indexes = buildInMemAdjEdges(relLabelMetadata);
    logger->info("Populating AdjEdges Indexes...");
    for (auto blockId = 0u; blockId < relLabelMetadata.numBlocks; blockId++) {
        threadPool.execute(populateAdjEdgesAndCountRelsInAdjListsTask, &relLabelMetadata, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &dirLabelListSizes, indexes.get(),
            &nodeIDMaps, &catalog,
            catalog.getPropertyMapForRelLabel(relLabelMetadata.label).size() > 2, logger);
    }
    threadPool.wait();
    logger->info("Writing AdjEdges Indexes to disk...");
    for (auto dir : DIRECTIONS) {
        if (relLabelMetadata.isSingleCardinalityPerDir[dir]) {
            for (auto &col : *(*indexes)[dir]) {
                if (col) {
                    col->saveToFile();
                }
            }
        }
    }
}

unique_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>> RelsLoader::buildInMemAdjEdges(
    RelLabelMetadata &relLabelMetadata) {
    logger->info("Creating AdjEdges Indexes and Counting Rels for AdjLists Indexes...");
    auto indexes = make_unique<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>>(2);
    for (auto dir : DIRECTIONS) {
        if (relLabelMetadata.isSingleCardinalityPerDir[dir]) {
            (*indexes)[dir] =
                make_unique<vector<unique_ptr<InMemAdjEdges>>>(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : relLabelMetadata.nodeLabelsPerDir[dir]) {
                auto fname = AdjEdges::getAdjEdgesIndexFname(
                    outputDirectory, relLabelMetadata.label, boundNodeLabel, dir);
                (*(*indexes)[dir])[boundNodeLabel] =
                    make_unique<InMemAdjEdges>(fname, graph.getNumNodesForLabel(boundNodeLabel),
                        relLabelMetadata.numBytesSchemePerDir[dir].first,
                        relLabelMetadata.numBytesSchemePerDir[dir].second);
            }
        }
    }
    return indexes;
}

void RelsLoader::contructAdjLists(RelLabelMetadata &relLabelMetadata,
    dirLabelListSizes_t &dirLabelListSizes, dirLabelAdjListsMetadata_t &dirLabelAdjListsMetadata) {
    initAdjListsMetadata(relLabelMetadata, dirLabelListSizes, dirLabelAdjListsMetadata);
    populateAdjLists(relLabelMetadata, dirLabelListSizes, dirLabelAdjListsMetadata);
}

void RelsLoader::initAdjListsMetadata(RelLabelMetadata &relLabelMetadata,
    dirLabelListSizes_t &dirLabelListSizes, dirLabelAdjListsMetadata_t &dirLabelAdjListsMetadata) {
    logger->info("Allocating AdjLists pages...");
    for (auto direction : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[direction]) {
            auto edgeSize = relLabelMetadata.numBytesSchemePerDir[direction].first +
                            relLabelMetadata.numBytesSchemePerDir[direction].second;
            for (auto nodeLabel : relLabelMetadata.nodeLabelsPerDir[direction]) {
                threadPool.execute(initAdjListsMetadataForAnIndexTask,
                    graph.getNumNodesForLabel(nodeLabel), PAGE_SIZE / edgeSize,
                    dirLabelListSizes[direction][nodeLabel].get(),
                    &dirLabelAdjListsMetadata[direction][nodeLabel]);
            }
        }
    }
    threadPool.wait();
}

void RelsLoader::populateAdjLists(RelLabelMetadata &relLabelMetadata,
    dirLabelListSizes_t &dirLabelListSizes, dirLabelAdjListsMetadata_t &dirLabelAdjListsMetadata) {
    auto indexes = buildInMemAdjLists(relLabelMetadata, dirLabelAdjListsMetadata);
    logger->info("Populating AdjLists Indexes...");
    for (auto blockId = 0u; blockId < relLabelMetadata.numBlocks; blockId++) {
        threadPool.execute(populateAdjListsTask, &relLabelMetadata, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &dirLabelListSizes,
            &dirLabelAdjListsMetadata, indexes.get(), &nodeIDMaps, &catalog,
            catalog.getPropertyMapForRelLabel(relLabelMetadata.label).size() > 2, logger);
    }
    threadPool.wait();
    logger->info("Writing AdjLists Indexes to file...");
    for (auto dir : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[dir]) {
            for (auto nodeLabel : relLabelMetadata.nodeLabelsPerDir[dir]) {
                (*(*indexes)[dir])[nodeLabel]->saveToFile();
                auto fname = AdjLists::getAdjListsIndexFname(
                    outputDirectory, relLabelMetadata.label, nodeLabel, dir);
                dirLabelAdjListsMetadata[dir][nodeLabel].saveToFile(fname);
            }
        }
    }
}

unique_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>>
RelsLoader::buildInMemAdjLists(
    RelLabelMetadata &relLabelMetadata, dirLabelAdjListsMetadata_t &dirLabelAdjListsMetadata) {
    logger->info("Creating AdjLists...");
    auto indexes = make_unique<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>>(2);
    for (auto direction : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[direction]) {
            (*indexes)[direction] =
                make_unique<vector<unique_ptr<InMemAdjListsIndex>>>(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : relLabelMetadata.nodeLabelsPerDir[direction]) {
                auto fname = AdjLists::getAdjListsIndexFname(
                    outputDirectory, relLabelMetadata.label, boundNodeLabel, direction);
                (*(*indexes)[direction])[boundNodeLabel] = make_unique<InMemAdjListsIndex>(fname,
                    dirLabelAdjListsMetadata[direction][boundNodeLabel].numPages,
                    relLabelMetadata.numBytesSchemePerDir[direction].first,
                    relLabelMetadata.numBytesSchemePerDir[direction].second);
            }
        }
    }
    return indexes;
}

void RelsLoader::populateAdjEdgesAndCountRelsInAdjListsTask(RelLabelMetadata *relLabelMetadata,
    uint64_t blockId, const char tokenSeparator, dirLabelListSizes_t *dirLabelListSizes,
    vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>> *adjEdgesIndexes,
    vector<shared_ptr<NodeIDMap>> *nodeIDMaps, const Catalog *catalog, bool hasProperties,
    shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", relLabelMetadata->fname, blockId);
    CSVReader reader(relLabelMetadata->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    gfLabel_t labels[2];
    gfNodeOffset_t offsets[2];
    while (reader.hasNextLine()) {
        for (auto dir : DIRECTIONS) {
            reader.hasNextToken();
            labels[dir] = (*catalog).getNodeLabelFromString(*reader.getString());
            reader.hasNextToken();
            offsets[dir] = (*(*nodeIDMaps)[labels[dir]]).getOffset(*reader.getNodeID());
        }
        for (auto dir : DIRECTIONS) {
            if (relLabelMetadata->isSingleCardinalityPerDir[dir]) {
                (*(*adjEdgesIndexes)[dir])[labels[dir]]->set(
                    offsets[dir], labels[!dir], offsets[!dir]);
            } else {
                (*(*dirLabelListSizes)[dir][labels[dir]])[offsets[dir]].fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
        if (hasProperties) {
            reader.skipLine();
        }
    }
    logger->debug("end   {0} {1}", relLabelMetadata->fname, blockId);
}

void RelsLoader::initAdjListsMetadataForAnIndexTask(uint64_t numNodeOffsets,
    uint32_t numEdgesPerPage, listSizes_t *listSizes, AdjListsMetadata *adjListsMetadata) {
    (*adjListsMetadata).headers.resize(numNodeOffsets);
    auto globalPageId = 0u;
    auto numChunks = (numNodeOffsets / AdjLists::ADJLISTS_CHUNK_SIZE) + 1;
    (*adjListsMetadata).chunksPagesMap.resize(numChunks);
    gfNodeOffset_t nodeOffset = 0u;
    auto numLargeAdjLists = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto pageId = 0u, pageOffset = 0u;
        auto numElementsInChunk = min(nodeOffset + AdjLists::ADJLISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            if (relCount >= numEdgesPerPage) {
                numLargeAdjLists++;
            } else {
                (*listSizes)[nodeOffset].store(0, memory_order_relaxed);
                if (relCount + pageOffset > numEdgesPerPage) {
                    pageId++;
                    pageOffset = 0;
                }
                (*adjListsMetadata).headers[nodeOffset] =
                    ((pageId & 0x1ff) << 22) + ((pageOffset & 0x7ff) << 11) + (relCount & 0x7ff);
                pageOffset += relCount;
            }
            (*adjListsMetadata).chunksPagesMap[chunkId].resize(pageId + 1);
            nodeOffset++;
        }
        for (auto i = 0u; i < pageId + 1; i++) {
            (*adjListsMetadata).chunksPagesMap[chunkId][i] = globalPageId++;
        }
    }
    (*adjListsMetadata).lAdjListsPagesMap.resize(numLargeAdjLists);
    auto lAdjListsIdx = 0u;
    for (nodeOffset = 0; nodeOffset < numNodeOffsets; nodeOffset++) {
        auto relCount = (*listSizes)[nodeOffset].load();
        if (relCount > 0) {
            (*adjListsMetadata).headers[nodeOffset] = 0x80000000 + (lAdjListsIdx & 0x7fffffff);
            (*listSizes)[nodeOffset].store(0, memory_order_relaxed);
            auto numPages = relCount / numEdgesPerPage;
            if (relCount % numEdgesPerPage != 0) {
                numPages++;
            }
            (*adjListsMetadata).lAdjListsPagesMap[lAdjListsIdx].resize(numPages + 1);
            (*adjListsMetadata).lAdjListsPagesMap[lAdjListsIdx][0] = relCount;
            for (auto i = 1u; i <= numPages; i++) {
                (*adjListsMetadata).lAdjListsPagesMap[lAdjListsIdx][i] = globalPageId++;
            }
            lAdjListsIdx++;
        }
    }
    (*adjListsMetadata).numPages = globalPageId;
}

void RelsLoader::populateAdjListsTask(RelLabelMetadata *relLabelMetadata, uint64_t blockId,
    const char tokenSeparator, dirLabelListSizes_t *dirLabelListSizes,
    dirLabelAdjListsMetadata_t *dirLabelAdjListsMetadata,
    vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>> *adjListsIndexes,
    vector<shared_ptr<NodeIDMap>> *nodeIDMaps, const Catalog *catalog, bool hasProperties,
    shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", relLabelMetadata->fname, blockId);
    CSVReader reader(relLabelMetadata->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    gfLabel_t labels[2];
    gfNodeOffset_t offsets[2];
    while (reader.hasNextLine()) {
        for (auto dir : DIRECTIONS) {
            reader.hasNextToken();
            labels[dir] = (*catalog).getNodeLabelFromString(*reader.getString());
            reader.hasNextToken();
            offsets[dir] = (*(*nodeIDMaps)[labels[dir]]).getOffset(*reader.getNodeID());
        }
        for (auto dir : DIRECTIONS) {
            if (!relLabelMetadata->isSingleCardinalityPerDir[dir]) {
                auto boundNodeLabel = labels[dir];
                auto boundNodeOffset = offsets[dir];
                auto &adjListsMetadata = (*dirLabelAdjListsMetadata)[dir][boundNodeLabel];
                auto header = adjListsMetadata.headers[boundNodeOffset];
                auto pos = (*(*dirLabelListSizes)[dir][boundNodeLabel])[boundNodeOffset].fetch_add(
                    1, memory_order_relaxed);
                auto diskPageId = 0u, pageOffset = 0u;
                if (header & 0x80000000) {
                    auto edgeSize = relLabelMetadata->numBytesSchemePerDir[dir].first +
                                    relLabelMetadata->numBytesSchemePerDir[dir].second;
                    auto numElementsInAPages = PAGE_SIZE / edgeSize;
                    auto pageIdx = 1 + (pos / numElementsInAPages);
                    pos %= numElementsInAPages;
                    diskPageId = adjListsMetadata.lAdjListsPagesMap[header & 0x7fffffff][pageIdx];
                    pageOffset = 0;
                } else {
                    auto chunkId = boundNodeOffset / AdjLists::ADJLISTS_CHUNK_SIZE;
                    diskPageId = adjListsMetadata.chunksPagesMap[chunkId][(header >> 22) & 0x1ff];
                    pageOffset = (header >> 11) & 0x7ff;
                }
                (*(*adjListsIndexes)[dir])[boundNodeLabel]->set(
                    diskPageId, pageOffset, pos, labels[!dir], offsets[!dir]);
            }
        }
        if (hasProperties) {
            reader.skipLine();
        }
    }
    logger->debug("end   {0} {1}", relLabelMetadata->fname, blockId);
}

} // namespace common
} // namespace graphflow
