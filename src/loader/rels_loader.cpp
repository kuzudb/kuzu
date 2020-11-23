#include "src/loader/include/rels_loader.h"

#include "src/loader/include/csv_reader.h"

namespace graphflow {
namespace loader {

void RelsLoader::load(vector<string>& fnames, vector<uint64_t>& numBlocksPerFile) {
    RelLabelDescription description;
    for (auto relLabel = 0u; relLabel < catalog.getRelLabelsCount(); relLabel++) {
        auto& propertyMap = catalog.getPropertyMapForRelLabel(relLabel);
        description.propertyMap = &propertyMap;
        description.label = relLabel;
        description.fname = fnames[relLabel];
        description.numBlocks = numBlocksPerFile[relLabel];
        for (auto& dir : DIRS) {
            description.isSingleCardinalityPerDir[dir] =
                catalog.isSingleCaridinalityInDir(description.label, dir);
            description.nodeLabelsPerDir[dir] =
                catalog.getNodeLabelsForRelLabelDir(description.label, dir);
        }
        for (auto& dir : DIRS) {
            description.numBytesSchemePerDir[dir] =
                RelsStore::getNumBytesScheme(description.nodeLabelsPerDir[!dir],
                    graph.getNumNodesPerLabel(), catalog.getNodeLabelsCount());
        }
        loadRelsForLabel(description);
    }
}

void RelsLoader::loadRelsForLabel(RelLabelDescription& description) {
    logger->info("Creating Indexes for rel label {}.", description.label);
    dirLabelListSizes_t dirLabelListSizes{2};
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            dirLabelListSizes[dir].resize(catalog.getNodeLabelsCount());
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                dirLabelListSizes[dir][nodeLabel] =
                    make_unique<vector<atomic<uint64_t>>>(graph.getNumNodesForLabel(nodeLabel));
            }
        }
    }
    constructAdjRelsAndCountRelsInAdjLists(description, dirLabelListSizes);
    if (!description.isSingleCardinalityPerDir[FWD] ||
        !description.isSingleCardinalityPerDir[BWD]) {
        constructAdjLists(description, dirLabelListSizes);
    }
    logger->info("Done.");
}

void RelsLoader::constructAdjRelsAndCountRelsInAdjLists(
    RelLabelDescription& description, dirLabelListSizes_t& dirLabelListSizes) {
    AdjRelsLoaderHelper adjRelsLoaderHelper(description, graph, catalog, outputDirectory, logger);
    logger->info("Populating AdjRels and Rel Property Columns...");
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        threadPool.execute(populateAdjRelsAndCountRelsInAdjListsTask, &description, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &dirLabelListSizes,
            &adjRelsLoaderHelper, &nodeIDMaps, &catalog, logger);
    }
    threadPool.wait();
    logger->info("Writing AdjRels and Rel Property Columns to disk...");
    adjRelsLoaderHelper.saveToFile();
}

void RelsLoader::constructAdjLists(
    RelLabelDescription& description, dirLabelListSizes_t& dirLabelListSizes) {
    AdjListsLoaderHelper adjListsLoaderHelper(description, graph, catalog, outputDirectory, logger);
    initAdjListHeaders(description, dirLabelListSizes, adjListsLoaderHelper);
    initAdjListsAndPropertyListsMetadata(description, dirLabelListSizes, adjListsLoaderHelper);
    adjListsLoaderHelper.buildInMemStructures();
    logger->info("Populating AdjLists and Rel Property Lists...");
    for (auto blockId = 0u; blockId < description.numBlocks; blockId++) {
        threadPool.execute(populateAdjListsTask, &description, blockId,
            metadata.at("tokenSeparator").get<string>()[0], &dirLabelListSizes,
            &adjListsLoaderHelper, &nodeIDMaps, &catalog, logger);
    }
    threadPool.wait();
    logger->info("Writing AdjLists and Rel Property Lists to disk...");
    adjListsLoaderHelper.saveToFile();
}

void RelsLoader::initAdjListHeaders(RelLabelDescription& description,
    dirLabelListSizes_t& dirLabelListSizes, AdjListsLoaderHelper& adjListsLoaderHelper) {
    logger->info("Initializing AdjListHeaders...");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            auto relSize = description.numBytesSchemePerDir[dir].first +
                           description.numBytesSchemePerDir[dir].second;
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute(calculateAdjListHeadersForIndexTask, dir, nodeLabel,
                    graph.getNumNodesForLabel(nodeLabel), PAGE_SIZE / relSize,
                    dirLabelListSizes[dir][nodeLabel].get(),
                    &adjListsLoaderHelper.getAdjListHeaders(dir, nodeLabel));
            }
        }
    }
    threadPool.wait();
}

void RelsLoader::initAdjListsAndPropertyListsMetadata(RelLabelDescription& description,
    dirLabelListSizes_t& dirLabelListSizes, AdjListsLoaderHelper& adjListsLoaderHelper) {
    logger->info("Initializing AdjLists and PropertyLists Metadata...");
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            auto relSize = description.numBytesSchemePerDir[dir].first +
                           description.numBytesSchemePerDir[dir].second;
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute(calculateListsMetadataForListsTask,
                    graph.getNumNodesForLabel(nodeLabel), PAGE_SIZE / relSize,
                    dirLabelListSizes[dir][nodeLabel].get(),
                    &adjListsLoaderHelper.getAdjListHeaders(dir, nodeLabel),
                    &adjListsLoaderHelper.getAdjListsMetadata(dir, nodeLabel));
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto& dir : DIRS) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                auto listsSizes = dirLabelListSizes[dir][nodeLabel].get();
                auto adjListHeaders = &adjListsLoaderHelper.getAdjListHeaders(dir, nodeLabel);
                auto numNodeOffsets = graph.getNumNodesForLabel(nodeLabel);
                for (auto i = 0u; i < description.propertyMap->size(); i++) {
                    auto& property = (*description.propertyMap)[i];
                    if (NODE != property.dataType && LABEL != property.dataType &&
                        STRING != property.dataType) {
                        auto numPerPage = PAGE_SIZE / getDataTypeSize(property.dataType);
                        threadPool.execute(calculateListsMetadataForListsTask, numNodeOffsets,
                            numPerPage, listsSizes, adjListHeaders,
                            &adjListsLoaderHelper.getPropertyListsMetadata(dir, nodeLabel, i));
                    }
                }
            }
        }
    }
    threadPool.wait();
}

void RelsLoader::populateAdjRelsAndCountRelsInAdjListsTask(RelLabelDescription* description,
    uint64_t blockId, const char tokenSeparator, dirLabelListSizes_t* dirLabelListSizes,
    AdjRelsLoaderHelper* adjRelsLoaderHelper, vector<shared_ptr<NodeIDMap>>* nodeIDMaps,
    const Catalog* catalog, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", description->fname, blockId);
    CSVReader reader(description->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    gfLabel_t labels[2];
    gfNodeOffset_t offsets[2];
    while (reader.hasNextLine()) {
        for (auto& dir : DIRS) {
            reader.hasNextToken();
            labels[dir] = (*catalog).getNodeLabelFromString(*reader.getString());
            reader.hasNextToken();
            offsets[dir] = (*(*nodeIDMaps)[labels[dir]]).getOffset(*reader.getNodeID());
        }
        for (auto& dir : DIRS) {
            if (description->isSingleCardinalityPerDir[dir]) {
                (*adjRelsLoaderHelper)
                    .getAdjRels(dir, labels[dir])
                    .set(offsets[dir], labels[!dir], offsets[!dir]);
            } else {
                (*(*dirLabelListSizes)[dir][labels[dir]])[offsets[dir]].fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
        if (description->hasProperties() && !description->requirePropertyLists()) {
            if (description->isSingleCardinalityPerDir[FWD]) {
                putPropsOfLineIntoInMemPropertyColumns((*description).propertyMap, reader,
                    adjRelsLoaderHelper, labels[FWD], offsets[FWD]);
            } else if (description->isSingleCardinalityPerDir[BWD]) {
                putPropsOfLineIntoInMemPropertyColumns((*description).propertyMap, reader,
                    adjRelsLoaderHelper, labels[BWD], offsets[BWD]);
            }
        }
    }
    logger->debug("end   {0} {1}", description->fname, blockId);
}

void RelsLoader::calculateAdjListHeadersForIndexTask(Direction dir, gfLabel_t nodeLabel,
    gfNodeOffset_t numNodeOffsets, uint32_t numPerPage, listSizes_t* listSizes,
    AdjListHeaders* adjListHeaders) {
    auto numChunks = (numNodeOffsets / Lists::LISTS_CHUNK_SIZE) + 1;
    gfNodeOffset_t nodeOffset = 0u;
    uint64_t lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto csrOffset = 0u;
        auto numElementsInChunk = min(nodeOffset + Lists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            uint32_t header;
            if (relCount >= numPerPage) {
                header = 0x80000000 + (lAdjListsIdx++ & 0x7fffffff);
            } else {
                header = ((csrOffset & 0xfffff) << 11) + (relCount & 0x7ff);
                csrOffset += relCount;
            }
            (*adjListHeaders).headers[nodeOffset] = header;
            nodeOffset++;
        }
    }
}

void RelsLoader::calculateListsMetadataForListsTask(uint64_t numNodeOffsets, uint32_t numPerPage,
    listSizes_t* listSizes, AdjListHeaders* adjListHeaders, ListsMetadata* adjListsMetadata) {
    auto globalPageId = 0u;
    auto numChunks = (numNodeOffsets / Lists::LISTS_CHUNK_SIZE) + 1;
    (*adjListsMetadata).chunksPagesMap.resize(numChunks);
    gfNodeOffset_t nodeOffset = 0u;
    auto lAdjListsIdx = 0u;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        auto pageId = 0u, csrOffsetInPage = 0u;
        auto numElementsInChunk = min(nodeOffset + Lists::LISTS_CHUNK_SIZE, numNodeOffsets);
        for (auto i = nodeOffset; i < numElementsInChunk; i++) {
            auto relCount = (*listSizes)[nodeOffset].load(memory_order_relaxed);
            if ((*adjListHeaders).headers[nodeOffset] & 0x80000000) {
                (*adjListsMetadata).largeListsPagesMap.resize(lAdjListsIdx + 1);
                auto numPages = relCount / numPerPage;
                if (relCount % numPerPage != 0) {
                    numPages++;
                }
                (*adjListsMetadata).largeListsPagesMap[lAdjListsIdx].resize(numPages + 1);
                (*adjListsMetadata).largeListsPagesMap[lAdjListsIdx][0] = relCount;
                for (auto i = 1u; i <= numPages; i++) {
                    (*adjListsMetadata).largeListsPagesMap[lAdjListsIdx][i] = globalPageId++;
                }
                lAdjListsIdx++;
            } else {
                while (relCount + csrOffsetInPage > numPerPage) {
                    relCount -= (numPerPage - csrOffsetInPage);
                    pageId++;
                    csrOffsetInPage = 0;
                }
                csrOffsetInPage += relCount;
            }

            nodeOffset++;
        }
        if (pageId != 0 || csrOffsetInPage != 0) {
            (*adjListsMetadata).chunksPagesMap[chunkId].resize(pageId + 1);
            for (auto i = 0u; i < pageId + 1; i++) {
                (*adjListsMetadata).chunksPagesMap[chunkId][i] = globalPageId++;
            }
        }
    }
    (*adjListsMetadata).numPages = globalPageId;
}

void RelsLoader::populateAdjListsTask(RelLabelDescription* description, uint64_t blockId,
    const char tokenSeparator, dirLabelListSizes_t* dirLabelListSizes,
    AdjListsLoaderHelper* adjListsLoaderHelper, vector<shared_ptr<NodeIDMap>>* nodeIDMaps,
    const Catalog* catalog, shared_ptr<spdlog::logger> logger) {
    logger->debug("start {0} {1}", description->fname, blockId);
    CSVReader reader(description->fname, tokenSeparator, blockId);
    if (0 == blockId) {
        if (reader.hasNextLine()) {
            reader.skipLine(); // skip header line
        }
    }
    vector<gfLabel_t> labels{2};
    vector<gfNodeOffset_t> offsets{2};
    vector<uint64_t> pos{2};
    vector<uint32_t> headers{2};
    while (reader.hasNextLine()) {
        for (auto& dir : DIRS) {
            reader.hasNextToken();
            labels[dir] = (*catalog).getNodeLabelFromString(*reader.getString());
            reader.hasNextToken();
            offsets[dir] = (*(*nodeIDMaps)[labels[dir]]).getOffset(*reader.getNodeID());
        }
        for (auto& dir : DIRS) {
            if (!description->isSingleCardinalityPerDir[dir]) {
                headers[dir] = (*adjListsLoaderHelper)
                                   .getAdjListHeaders(dir, labels[dir])
                                   .headers[offsets[dir]];
                pos[dir] = (*(*dirLabelListSizes)[dir][labels[dir]])[offsets[dir]].fetch_sub(
                    1, memory_order_relaxed);
                auto relSize = description->getRelSize(dir);
                uint64_t pageID = 0u, csrOffsetInPage = 0u;
                calcPageIDAndCSROffsetInPage(headers[dir], pos[dir], PAGE_SIZE / relSize,
                    offsets[dir], pageID, csrOffsetInPage,
                    (*adjListsLoaderHelper).getAdjListsMetadata(dir, labels[dir]));
                (*adjListsLoaderHelper)
                    .getAdjLists(dir, labels[dir])
                    .set(pageID, csrOffsetInPage, labels[!dir], offsets[!dir]);
            }
        }
        if (description->requirePropertyLists()) {
            putPropsOfLineIntoInMemRelPropLists(description->propertyMap, reader, labels, offsets,
                pos, headers, adjListsLoaderHelper);
        }
    }
    logger->debug("end   {0} {1}", description->fname, blockId);
}

void RelsLoader::putPropsOfLineIntoInMemPropertyColumns(const vector<Property>* propertyMap,
    CSVReader& reader, AdjRelsLoaderHelper* adjRelsLoaderHelper, gfLabel_t nodeLabel,
    gfNodeOffset_t nodeOffset) {
    auto propertyIdx = 0;
    while (reader.hasNextToken()) {
        switch ((*propertyMap)[propertyIdx].dataType) {
        case INT: {
            auto intVal = reader.skipTokenIfNull() ? NULL_GFINT : reader.getInteger();
            (*adjRelsLoaderHelper)
                .getPropertyColumn(nodeLabel, propertyIdx)
                .set(nodeOffset, reinterpret_cast<byte*>(&intVal));
            break;
        }
        case DOUBLE: {
            auto doubleVal = reader.skipTokenIfNull() ? NULL_GFINT : reader.getDouble();
            (*adjRelsLoaderHelper)
                .getPropertyColumn(nodeLabel, propertyIdx)
                .set(nodeOffset, reinterpret_cast<byte*>(&doubleVal));
            break;
        }
        case BOOLEAN: {
            auto boolVal = reader.skipTokenIfNull() ? NULL_GFINT : reader.getBoolean();
            (*adjRelsLoaderHelper)
                .getPropertyColumn(nodeLabel, propertyIdx)
                .set(nodeOffset, reinterpret_cast<byte*>(&boolVal));
            break;
        }
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
        propertyIdx++;
    }
}

void RelsLoader::calcPageIDAndCSROffsetInPage(uint32_t header, uint64_t pos,
    uint64_t numElementsInAPage, gfNodeOffset_t nodeOffset, uint64_t& pageID,
    uint64_t& csrOffsetInPage, ListsMetadata& metadata) {
    if (header & 0x80000000) {
        // if the corresponding AdjList is large.
        pos = metadata.largeListsPagesMap[header & 0x7fffffff][0] - pos;
        auto pageIdx = 1 + (pos / numElementsInAPage);
        csrOffsetInPage = pos % numElementsInAPage;
        pageID = metadata.largeListsPagesMap[header & 0x7fffffff][pageIdx];
        return;
    }
    auto chunkId = nodeOffset / Lists::LISTS_CHUNK_SIZE;
    auto csrOffset = (header >> 11) & 0xfffff;
    pos = (header & 0x7ff) - pos;
    pageID = metadata.chunksPagesMap[chunkId][(csrOffset + pos) / numElementsInAPage];
    csrOffsetInPage = (csrOffset + pos) % numElementsInAPage;
}

void RelsLoader::putPropsOfLineIntoInMemRelPropLists(const vector<Property>* propertyMap,
    CSVReader& reader, vector<gfLabel_t>& labels, vector<gfNodeOffset_t>& offsets,
    vector<uint64_t>& pos, vector<uint32_t>& headers, AdjListsLoaderHelper* adjListsLoaderHelper) {
    auto propertyIdx = 0;
    while (reader.hasNextToken()) {
        switch ((*propertyMap)[propertyIdx].dataType) {
        case INT: {
            auto intVal = reader.skipTokenIfNull() ? NULL_GFINT : reader.getInteger();
            for (auto& dir : DIRS) {
                setValInAnInMemRelPropLists(headers[dir], pos[dir], getDataTypeSize(INT),
                    offsets[dir], reinterpret_cast<byte*>(&intVal),
                    (*adjListsLoaderHelper).getPropertyListsMetadata(dir, labels[dir], propertyIdx),
                    (*adjListsLoaderHelper).getPropertyLists(dir, labels[dir], propertyIdx));
            }
            break;
        }
        case DOUBLE: {
            auto doubleVal = reader.skipTokenIfNull() ? NULL_GFDOUBLE : reader.getDouble();
            for (auto& dir : DIRS) {
                setValInAnInMemRelPropLists(headers[dir], pos[dir], getDataTypeSize(DOUBLE),
                    offsets[dir], reinterpret_cast<byte*>(&doubleVal),
                    (*adjListsLoaderHelper).getPropertyListsMetadata(dir, labels[dir], propertyIdx),
                    (*adjListsLoaderHelper).getPropertyLists(dir, labels[dir], propertyIdx));
            }
            break;
        }
        case BOOLEAN: {
            auto boolVal = reader.skipTokenIfNull() ? NULL_GFBOOL : reader.getBoolean();
            for (auto& dir : DIRS) {
                setValInAnInMemRelPropLists(headers[dir], pos[dir], getDataTypeSize(BOOLEAN),
                    offsets[dir], reinterpret_cast<byte*>(&boolVal),
                    (*adjListsLoaderHelper).getPropertyListsMetadata(dir, labels[dir], propertyIdx),
                    (*adjListsLoaderHelper).getPropertyLists(dir, labels[dir], propertyIdx));
            }
            break;
        }
        default:
            if (!reader.skipTokenIfNull()) {
                reader.skipToken();
            }
        }
        propertyIdx++;
    }
}

void RelsLoader::setValInAnInMemRelPropLists(uint32_t header, uint64_t pos, uint8_t elementSize,
    gfNodeOffset_t& offset, byte* val, ListsMetadata& listsMetadata,
    InMemPropertyLists& propertyLists) {
    uint64_t pageIdx = 0, pageOffset = 0;
    calcPageIDAndCSROffsetInPage(
        header, pos, PAGE_SIZE / elementSize, offset, pageIdx, pageOffset, listsMetadata);
    propertyLists.set(pageIdx, pageOffset, val);
}

} // namespace loader
} // namespace graphflow
