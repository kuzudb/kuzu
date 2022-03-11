#pragma once

#include "src/common/include/compression_scheme.h"
#include "src/loader/include/dataset_metadata.h"
#include "src/loader/include/node_id_map.h"
#include "src/storage/include/catalog.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

struct LabelDescription {

    explicit LabelDescription(label_t label, string fName, uint64_t numBlocks,
        const vector<PropertyDefinition>& properties, const CSVReaderConfig& csvReaderConfig)
        : label{label}, fName{std::move(fName)}, numBlocks{numBlocks}, properties{properties},
          csvReaderConfig(csvReaderConfig){};

    label_t label;
    string fName;
    uint64_t numBlocks;
    const vector<PropertyDefinition>& properties;
    CSVReaderConfig csvReaderConfig;
};

struct RelLabelDescription : public LabelDescription {

    explicit RelLabelDescription(label_t label, string fName, uint64_t numBlocks,
        const vector<PropertyDefinition>& properties, const CSVReaderConfig& csvReaderConfig)
        : LabelDescription(label, std::move(fName), numBlocks, properties, csvReaderConfig) {}

    bool hasProperties() { return !properties.empty(); }

    bool requirePropertyLists() {
        return hasProperties() && !isSingleMultiplicityPerDirection[FWD] &&
               !isSingleMultiplicityPerDirection[BWD];
    };

    vector<unordered_set<label_t>> nodeLabelsPerDirection{2};
    vector<bool> isSingleMultiplicityPerDirection{false, false};
    vector<NodeIDCompressionScheme> nodeIDCompressionSchemePerDirection{2};
};

struct NodeLabelDescription : public LabelDescription {

    explicit NodeLabelDescription(label_t label, string fName, uint64_t numBlocks,
        const vector<PropertyDefinition>& properties, const CSVReaderConfig& csvReaderConfig,
        NodeIDMap* nodeIDMap)
        : LabelDescription(label, std::move(fName), numBlocks, properties, csvReaderConfig),
          nodeIDMap(nodeIDMap){};

    NodeIDMap* nodeIDMap;
};

} // namespace loader
} // namespace graphflow
