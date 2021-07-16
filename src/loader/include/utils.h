#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>

#include "robin_hood.h"

#include "src/common/include/compression_scheme.h"
#include "src/common/include/configs.h"
#include "src/common/include/types.h"
#include "src/common/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/data_structure/lists/list_headers.h"
#include "src/storage/include/data_structure/lists/lists_metadata.h"
#include "src/storage/include/data_structure/lists/utils.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

// Maps the primary key of a node to the in-system used node offset.
class NodeIDMap {

public:
    NodeIDMap(const uint64_t& size) : size{size}, offsetToNodeIDMap(make_unique<char*[]>(size)){};
    ~NodeIDMap();

    void set(const char* nodeID, node_offset_t nodeOffset);

    node_offset_t get(const char* nodeID);

    inline void createNodeIDToOffsetMap() {
        nodeIDToOffsetMap.reserve(1.5 * size);
        for (auto i = 0u; i < size; i++) {
            nodeIDToOffsetMap.emplace(offsetToNodeIDMap[i], i);
        }
    }

private:
    uint64_t size;
    robin_hood::unordered_flat_map<const char*, node_offset_t, charArrayHasher, charArrayEqualTo>
        nodeIDToOffsetMap;
    unique_ptr<char*[]> offsetToNodeIDMap;
};

struct LabelFileDescription {
protected:
    LabelFileDescription(string filePath, string labelName)
        : filePath{move(filePath)}, labelName{move(labelName)} {}

public:
    string filePath;
    string labelName;
};

struct NodeFileDescription : public LabelFileDescription {
    NodeFileDescription(string filePath, string labelName, string primaryKeyPropertyName)
        : LabelFileDescription{move(filePath), move(labelName)}, primaryKeyPropertyName{move(
                                                                     primaryKeyPropertyName)} {}

    string primaryKeyPropertyName;
};

struct RelFileDescription : public LabelFileDescription {
    RelFileDescription(string filePath, string labelName, string relMultiplicity,
        vector<string> srcNodeLabelNames, vector<string> dstNodeLabelNames)
        : LabelFileDescription{move(filePath), move(labelName)}, relMultiplicity{relMultiplicity},
          srcNodeLabelNames{srcNodeLabelNames}, dstNodeLabelNames{dstNodeLabelNames} {}

    string relMultiplicity;
    vector<string> srcNodeLabelNames;
    vector<string> dstNodeLabelNames;
};

// Holds information about a rel label that is needed to construct adjRels and adjLists
// indexes, property columns, and property lists.
class RelLabelDescription {

public:
    explicit RelLabelDescription(const vector<PropertyDefinition>& properties)
        : properties{properties} {}

    bool hasProperties() { return !properties.empty(); }

    bool requirePropertyLists() {
        return hasProperties() && !isSingleMultiplicityPerDirection[FWD] &&
               !isSingleMultiplicityPerDirection[BWD];
    };

public:
    label_t label;
    string fName;
    uint64_t numBlocks;
    vector<unordered_set<label_t>> nodeLabelsPerDirection{2};
    vector<bool> isSingleMultiplicityPerDirection{false, false};
    vector<NodeIDCompressionScheme> nodeIDCompressionSchemePerDirection{2};
    const vector<PropertyDefinition>& properties;
};

// listSizes_t is the type of structure that is used to count the size of each list in the
// particular Lists data structure.
typedef vector<atomic<uint64_t>> listSizes_t;

// Helper functions to assist populating ListHeaders, ListsMetadata and in-memory representation of
// Lists.
class ListsLoaderHelper {

public:
    static inline void incrementListSize(listSizes_t& listSizes, uint32_t offset, uint32_t val) {
        listSizes[offset].fetch_add(val, memory_order_relaxed);
    }

    static inline uint64_t decrementListSize(
        listSizes_t& listSizes, uint32_t offset, uint32_t val) {
        return listSizes[offset].fetch_sub(val, memory_order_relaxed);
    }

    // Inits (in listHeaders) the header of each list in a Lists structure, from the listSizes.
    // ListSizes is used to determine if the list is small or large, based on which, information is
    // encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, ListHeaders* listHeaders, const shared_ptr<spdlog::logger>& logger);

    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeaders.
    static void calculateListsMetadataTask(uint64_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
        const shared_ptr<spdlog::logger>& logger);

    // Calculates the page id and offset in page where the data of a particular list has to be put
    // in the in-mem pages.
    static void calculatePageCursor(const uint32_t& header, const uint64_t& reversePos,
        const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor,
        ListsMetadata& metadata);
};

} // namespace loader
} // namespace graphflow
