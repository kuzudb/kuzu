#include "src/common/loader/include/rels_loader.h"

#include "src/common/loader/include/csv_reader.h"
#include "src/storage/include/indexes.h"

namespace graphflow {
namespace common {

void RelsLoader::load(vector<string> &fnames, unique_ptr<vector<shared_ptr<NodeIDMap>>> nodeIDMaps,
    vector<uint64_t> &numBlocksPerLabel) {
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
            relLabelMetadata.nodeIDMapsPerDir[dir].clear();
            for (auto nodeLabel : relLabelMetadata.nodeLabelsPerDir[dir]) {
                relLabelMetadata.nodeIDMapsPerDir[dir].push_back((*nodeIDMaps)[nodeLabel]);
            }
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
    dirLabelAdjListsIndexMetadata_t dirLabelAdjListsIndexMetadata{2};
    for (auto dir : DIRECTIONS) {
        dirLabelAdjListsIndexMetadata[dir].resize(catalog.getNodeLabelsCount());
        if (!relLabelMetadata.isSingleCardinalityPerDir[dir]) {
            for (auto nodeLabel : relLabelMetadata.nodeLabelsPerDir[dir]) {
                dirLabelAdjListsIndexMetadata[dir][nodeLabel].listSizes =
                    make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesForLabel(nodeLabel));
            }
        }
    }
    constructAdjEdgesAndCountRelsInAdjLists(relLabelMetadata, dirLabelAdjListsIndexMetadata);
    if (!relLabelMetadata.isSingleCardinalityPerDir[FORWARD] ||
        !relLabelMetadata.isSingleCardinalityPerDir[BACKWARD]) {
        contructAdjLists(relLabelMetadata, dirLabelAdjListsIndexMetadata);
    }
    logger->info("done.");
}

void RelsLoader::constructAdjEdgesAndCountRelsInAdjLists(RelLabelMetadata &relLabelMetadata,
    dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata) {
    auto indexes = buildInMemAdjEdges(relLabelMetadata);
    logger->info("Populating AdjEdges Indexes...");
    for (auto blockId = 0u; blockId < relLabelMetadata.numBlocks; blockId++) {
        threadPool.execute(populateAdjEdgesAndCountRelsInAdjListsTask, &relLabelMetadata, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &dirLabelAdjListsIndexMetadata, indexes,
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

shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>> RelsLoader::buildInMemAdjEdges(
    RelLabelMetadata &relLabelMetadata) {
    logger->info("Creating AdjEdges Indexes and Counting Rels for AdjLists Indexes...");
    auto indexes = make_shared<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>>(2);
    for (auto dir : DIRECTIONS) {
        if (relLabelMetadata.isSingleCardinalityPerDir[dir]) {
            (*indexes)[dir] =
                make_unique<vector<unique_ptr<InMemAdjEdges>>>(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : relLabelMetadata.nodeLabelsPerDir[dir]) {
                auto fname = Indexes::getAdjEdgesIndexFname(
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
    dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata) {
    allocateAdjListsPages(relLabelMetadata, dirLabelAdjListsIndexMetadata);
    populateAdjLists(relLabelMetadata, dirLabelAdjListsIndexMetadata);
    writeAdjListsHeadersAndPagesMaps(dirLabelAdjListsIndexMetadata);
}

void RelsLoader::allocateAdjListsPages(RelLabelMetadata &relLabelMetadata,
    dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata) {
    logger->info("Allocating AdjLists pages...");
    for (auto direction : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[direction]) {
            auto edgeSize = relLabelMetadata.numBytesSchemePerDir[direction].first +
                            relLabelMetadata.numBytesSchemePerDir[direction].second;
            for (auto nodeLabel : relLabelMetadata.nodeLabelsPerDir[direction]) {
                dirLabelAdjListsIndexMetadata[direction][nodeLabel].headers.resize(
                    graph.getNumNodesForLabel(nodeLabel));
                dirLabelAdjListsIndexMetadata[direction][nodeLabel].numPages =
                    allocateAdjListsPagesForAnIndex(graph.getNumNodesForLabel(nodeLabel),
                        PAGE_SIZE / edgeSize, dirLabelAdjListsIndexMetadata[direction][nodeLabel]);
            }
        }
    }
}

uint64_t RelsLoader::allocateAdjListsPagesForAnIndex(uint64_t numNodeOffsets,
    uint32_t numEdgesPerPage, AdjListsIndexMetadata &adjListsIndexMetadata) {
    auto globalPageId = 0u;
    auto numChunks = (numNodeOffsets / Indexes::ADJLISTS_CHUNK_SIZE) + 1;
    adjListsIndexMetadata.chunksPagesMap.resize(numChunks);
    gfNodeOffset_t nodeOffset = 0u;
    auto numLargeAdjLists = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto pageId = 0u, pageOffset = 0u;
        auto numElementsInChunk = min(nodeOffset + Indexes::ADJLISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount =
                (*adjListsIndexMetadata.listSizes)[nodeOffset].load(memory_order_relaxed);
            if (relCount >= numEdgesPerPage) {
                numLargeAdjLists++;
            } else {
                (*adjListsIndexMetadata.listSizes)[nodeOffset].store(0, memory_order_relaxed);
                if (relCount + pageOffset > numEdgesPerPage) {
                    pageId++;
                    pageOffset = 0;
                }
                adjListsIndexMetadata.headers[nodeOffset] =
                    ((pageId & 0x1ff) << 22) + ((pageOffset & 0x7ff) << 11) + (relCount & 0x7ff);
                pageOffset += relCount;
            }
            adjListsIndexMetadata.chunksPagesMap[chunkId].resize(pageId + 1);
            nodeOffset++;
        }
        for (auto i = 0u; i < pageId + 1; i++) {
            adjListsIndexMetadata.chunksPagesMap[chunkId][i] = globalPageId++;
        }
    }
    adjListsIndexMetadata.lAdjListsPagesMap.resize(numLargeAdjLists);
    auto lAdjListsIdx = 0u;
    for (nodeOffset = 0; nodeOffset < numNodeOffsets; nodeOffset++) {
        auto relCount = (*adjListsIndexMetadata.listSizes)[nodeOffset].load();
        if (relCount > 0) {
            adjListsIndexMetadata.headers[nodeOffset] = 0x80000000 + (lAdjListsIdx & 0x7fffffff);
            (*adjListsIndexMetadata.listSizes)[nodeOffset].store(0, memory_order_relaxed);
            auto numPages = relCount / numEdgesPerPage;
            if (relCount % numEdgesPerPage != 0) {
                numPages++;
            }
            adjListsIndexMetadata.lAdjListsPagesMap[lAdjListsIdx].resize(numPages + 1);
            adjListsIndexMetadata.lAdjListsPagesMap[lAdjListsIdx][0] = relCount;
            for (auto i = 1u; i <= numPages; i++) {
                adjListsIndexMetadata.lAdjListsPagesMap[lAdjListsIdx][i] = globalPageId++;
            }
            lAdjListsIdx++;
        }
    }
    return globalPageId;
}

void RelsLoader::populateAdjLists(RelLabelMetadata &relLabelMetadata,
    dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata) {
    auto indexes = buildInMemAdjLists(relLabelMetadata, dirLabelAdjListsIndexMetadata);
    logger->info("Populating AdjLists Indexes...");
    for (auto blockId = 0u; blockId < relLabelMetadata.numBlocks; blockId++) {
        threadPool.execute(populateAdjListsTask, &relLabelMetadata, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &dirLabelAdjListsIndexMetadata, indexes,
            catalog.getPropertyMapForRelLabel(relLabelMetadata.label).size() > 2, logger);
    }
    threadPool.wait();
    logger->info("Writing AdjLists Indexes to file...");
    for (auto direction : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[direction]) {
            for (auto &col : *(*indexes)[direction]) {
                if (col) {
                    col->saveToFile();
                }
            }
        }
    }
}

shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>>
RelsLoader::buildInMemAdjLists(RelLabelMetadata &relLabelMetadata,
    dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata) {
    logger->info("Creating AdjLists...");
    auto indexes = make_shared<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>>(2);
    for (auto direction : DIRECTIONS) {
        if (!relLabelMetadata.isSingleCardinalityPerDir[direction]) {
            (*indexes)[direction] =
                make_unique<vector<unique_ptr<InMemAdjListsIndex>>>(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : relLabelMetadata.nodeLabelsPerDir[direction]) {
                auto fname = Indexes::getAdjListsIndexFname(
                    outputDirectory, relLabelMetadata.label, boundNodeLabel, direction);
                (*(*indexes)[direction])[boundNodeLabel] = make_unique<InMemAdjListsIndex>(fname,
                    dirLabelAdjListsIndexMetadata[direction][boundNodeLabel].numPages,
                    relLabelMetadata.numBytesSchemePerDir[direction].first,
                    relLabelMetadata.numBytesSchemePerDir[direction].second);
            }
        }
    }
    return indexes;
}

void RelsLoader::writeAdjListsHeadersAndPagesMaps(
    dirLabelAdjListsIndexMetadata_t &dirLabelAdjListsIndexMetadata) {
    // nothing here currently.
}

void RelsLoader::populateAdjEdgesAndCountRelsInAdjListsTask(RelLabelMetadata *relLabelMetadata,
    uint64_t blockId, const char tokenSeparator,
    dirLabelAdjListsIndexMetadata_t *dirLabelAdjListsIndexMetadata,
    shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjEdges>>>>> adjEdgesIndexes,
    bool hasProperties, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", relLabelMetadata->fname, blockId);
    CSVReader reader(relLabelMetadata->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        reader.skipLine(); // skip header line
    }
    gfLabel_t labels[2];
    gfNodeOffset_t offsets[2];
    while (reader.hasMore()) {
        for (auto direction : DIRECTIONS) {
            resolveNodeIDMapIdxAndOffset(*reader.getNodeID(),
                relLabelMetadata->nodeLabelsPerDir[direction],
                relLabelMetadata->nodeIDMapsPerDir[direction], labels[direction],
                offsets[direction]);
        }
        for (auto direction : DIRECTIONS) {
            if (relLabelMetadata->isSingleCardinalityPerDir[direction]) {
                auto revDirection = !direction;
                (*(*adjEdgesIndexes)[direction])[labels[direction]]->set(
                    offsets[direction], labels[revDirection], offsets[revDirection]);
            } else {
                (*(*dirLabelAdjListsIndexMetadata)[direction][labels[direction]]
                        .listSizes)[offsets[direction]]
                    .fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (hasProperties) {
            reader.skipLine();
        }
    }
    logger->debug("end   {0} {1}", relLabelMetadata->fname, blockId);
}

void RelsLoader::populateAdjListsTask(RelLabelMetadata *relLabelMetadata, uint64_t blockId,
    const char tokenSeparator, dirLabelAdjListsIndexMetadata_t *dirLabelAdjListsIndexMetadata,
    shared_ptr<vector<unique_ptr<vector<unique_ptr<InMemAdjListsIndex>>>>> adjListsIndexes,
    bool hasProperties, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", relLabelMetadata->fname, blockId);
    CSVReader reader(relLabelMetadata->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        reader.skipLine(); // skip header line
    }
    gfLabel_t labels[2];
    gfNodeOffset_t offsets[2];
    while (reader.hasMore()) {
        for (auto direction : DIRECTIONS) {
            resolveNodeIDMapIdxAndOffset(*reader.getNodeID(),
                relLabelMetadata->nodeLabelsPerDir[direction],
                relLabelMetadata->nodeIDMapsPerDir[direction], labels[direction],
                offsets[direction]);
        }
        for (auto direction : DIRECTIONS) {
            if (!relLabelMetadata->isSingleCardinalityPerDir[direction]) {
                auto boundNodeLabel = labels[direction];
                auto boundNodeOffset = offsets[direction];
                auto &indexMetadata = (*dirLabelAdjListsIndexMetadata)[direction][boundNodeLabel];
                auto header = indexMetadata.headers[boundNodeOffset];
                auto pos =
                    (*indexMetadata.listSizes)[boundNodeOffset].fetch_add(1, memory_order_relaxed);
                auto diskPageId = 0u, pageOffset = 0u;
                if (header & 0x80000000) {
                    auto edgeSize = relLabelMetadata->numBytesSchemePerDir[direction].first +
                                    relLabelMetadata->numBytesSchemePerDir[direction].second;
                    auto numElementsInAPages = PAGE_SIZE / edgeSize;
                    auto pageIdx = 1 + (pos / numElementsInAPages);
                    pos %= numElementsInAPages;
                    diskPageId = indexMetadata.lAdjListsPagesMap[header & 0x7fffffff][pageIdx];
                    pageOffset = 0;
                } else {
                    auto chunkId = boundNodeOffset / Indexes::ADJLISTS_CHUNK_SIZE;
                    diskPageId = indexMetadata.chunksPagesMap[chunkId][(header >> 22) & 0x1ff];
                    pageOffset = (header >> 11) & 0x7ff;
                }
                (*(*adjListsIndexes)[direction])[boundNodeLabel]->set(
                    diskPageId, pageOffset, pos, labels[!direction], offsets[!direction]);
            }
        }
        if (hasProperties) {
            reader.skipLine();
        }
    }
    logger->debug("end   {0} {1}", relLabelMetadata->fname, blockId);
}

void RelsLoader::resolveNodeIDMapIdxAndOffset(string &nodeID, vector<gfLabel_t> &nodeLabels,
    vector<shared_ptr<NodeIDMap>> &nodeIDMaps, gfLabel_t &label, gfNodeOffset_t &offset) {
    auto i = 0;
    while (!nodeIDMaps[i]->hasNodeID(nodeID)) {
        i++;
    }
    offset = nodeIDMaps[i]->getOffset(nodeID);
    label = nodeLabels[i];
}

} // namespace common
} // namespace graphflow
