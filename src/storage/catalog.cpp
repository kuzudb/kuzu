#include "src/storage/include/catalog.h"

#include <fstream>
#include <iostream>

#include "bitsery/adapter/stream.h"
#include "bitsery/bitsery.h"
#include "bitsery/ext/std_map.h"
#include "bitsery/traits/string.h"
#include "bitsery/traits/vector.h"
#include "spdlog/spdlog.h"
#include <bitsery/brief_syntax.h>
#include <bitsery/brief_syntax/string.h>
#include <bitsery/ext/pointer.h>
#include <bitsery/ext/std_map.h>

using namespace std;
using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

Catalog::Catalog() {
    logger = spdlog::get("storage");
}

Catalog::Catalog(const string& directory) : Catalog() {
    logger->info("Initializing Catalog.");
    readFromFile(directory);
    logger->info("Done.");
};

const string Catalog::getStringNodeLabel(label_t label) const {
    for (stringToLabelMap_t::const_iterator it = stringToNodeLabelMap.begin();
         it != stringToNodeLabelMap.end(); ++it) {
        if (it->second == label) {
            return string(it->first);
        }
    }
    throw invalid_argument("No node label with labelIdx: " + label);
}

const vector<label_t>& Catalog::getRelLabelsForNodeLabelDirection(
    label_t nodeLabel, Direction dir) const {
    if (nodeLabel >= getNodeLabelsCount()) {
        throw invalid_argument("Node label out of the bounds.");
    }
    if (FWD == dir) {
        return srcNodeLabelToRelLabel[nodeLabel];
    }
    return dstNodeLabelToRelLabel[nodeLabel];
}

const vector<label_t>& Catalog::getNodeLabelsForRelLabelDir(label_t relLabel, Direction dir) const {
    if (relLabel >= getRelLabelsCount()) {
        throw invalid_argument("Node label out of the bounds.");
    }
    if (FWD == dir) {
        return relLabelToSrcNodeLabels[relLabel];
    }
    return relLabelToDstNodeLabels[relLabel];
}

bool Catalog::isSingleCaridinalityInDir(label_t relLabel, Direction dir) const {
    auto cardinality = relLabelToCardinalityMap[relLabel];
    if (FWD == dir) {
        return ONE_ONE == cardinality || MANY_ONE == cardinality;
    } else {
        return ONE_ONE == cardinality || ONE_MANY == cardinality;
    }
}

template<typename S>
void Catalog::serialize(S& s) {
    auto propertyMapsFunc = [](S& s, unordered_map<string, PropertyKey>& v) {
        s.ext(v, bitsery::ext::StdMap{UINT32_MAX},
            [](S& s, string& key, PropertyKey& value) { s(key, value.dataType, value.idx); });
    };
    s.container(nodePropertyKeyMaps, UINT32_MAX, propertyMapsFunc);
    s.container(relPropertyKeyMaps, UINT32_MAX, propertyMapsFunc);
    s.container(nodeUnstrPropertyKeyMaps, UINT32_MAX, propertyMapsFunc);

    auto vectorLabelsFunc = [](S& s, vector<label_t>& v) {
        s.container(v, UINT32_MAX, [](S& s, label_t& w) { s(w); });
    };
    s.container(relLabelToSrcNodeLabels, UINT32_MAX, vectorLabelsFunc);
    s.container(relLabelToDstNodeLabels, UINT32_MAX, vectorLabelsFunc);
    s.container(srcNodeLabelToRelLabel, UINT32_MAX, vectorLabelsFunc);
    s.container(dstNodeLabelToRelLabel, UINT32_MAX, vectorLabelsFunc);

    s.container(relLabelToCardinalityMap, UINT32_MAX, [](S& s, Cardinality& v) { s(v); });
}

void Catalog::saveToFile(const string& path) {
    auto catalogPath = path + "/catalog.bin";
    fstream f{catalogPath, f.binary | f.trunc | f.out};
    if (f.fail()) {
        throw invalid_argument("cannot open " + catalogPath + " for writing");
    }
    serializeStringToLabelMap(f, stringToNodeLabelMap);
    serializeStringToLabelMap(f, stringToRelLabelMap);
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void Catalog::readFromFile(const string& directory) {
    auto path = directory + "/catalog.bin";
    logger->debug("Reading from {}.", path);
    fstream f{path, f.binary | f.in};
    if (f.fail()) {
        throw invalid_argument("Cannot open " + path + " for reading the catalog.");
    }
    deserializeStringToLabelMap(f, stringToNodeLabelMap);
    deserializeStringToLabelMap(f, stringToRelLabelMap);
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (!(state.first == bitsery::ReaderError::NoError && state.second)) {
        throw invalid_argument("Cannot deserialize the catalog.");
    }
}

void Catalog::serializeStringToLabelMap(fstream& f, stringToLabelMap_t& map) {
    uint32_t mapSize = map.size();
    f.write(reinterpret_cast<char*>(&mapSize), sizeof(uint32_t));
    for (auto& entry : map) {
        mapSize = strlen(entry.first);
        f.write(reinterpret_cast<char*>(&mapSize), sizeof(uint32_t));
        for (auto i = 0u; i < strlen(entry.first); i++) {
            f << entry.first[i];
        }
        f.write(reinterpret_cast<char*>(&entry.second), sizeof(label_t));
    }
}

void Catalog::deserializeStringToLabelMap(fstream& f, stringToLabelMap_t& map) {
    uint32_t size;
    f.read(reinterpret_cast<char*>(&size), sizeof(uint32_t));
    for (auto i = 0u; i < size; i++) {
        uint32_t arraySize;
        f.read(reinterpret_cast<char*>(&arraySize), sizeof(uint32_t));
        auto array = new char[arraySize + 1];
        f.read(reinterpret_cast<char*>(array), arraySize);
        array[arraySize] = 0;
        label_t label;
        f.read(reinterpret_cast<char*>(&label), sizeof(label_t));
        map.insert({{array, label}});
    }
}

unique_ptr<nlohmann::json> Catalog::debugInfo() {
    auto json = make_unique<nlohmann::json>();
    for (stringToLabelMap_t::const_iterator labelIt = stringToNodeLabelMap.begin();
         labelIt != stringToNodeLabelMap.end(); ++labelIt) {
        string label(labelIt->first);
        (*json)["Catalog"]["NodeLabels"][label]["idx"] = to_string(labelIt->second);
        (*json)["Catalog"]["NodeLabels"][label]["properties"] =
            *getPropertiesJson(nodePropertyKeyMaps.at(labelIt->second));
    }

    for (stringToLabelMap_t::const_iterator labelIt = stringToRelLabelMap.begin();
         labelIt != stringToRelLabelMap.end(); ++labelIt) {
        string label(labelIt->first);
        (*json)["Catalog"]["RelLabels"][label]["idx"] = to_string(labelIt->second);
        (*json)["Catalog"]["RelLabels"][label]["cardinality"] =
            graphflow::common::CardinalityNames[relLabelToCardinalityMap[labelIt->second]];
        (*json)["Catalog"]["RelLabels"][label]["properties"] =
            *getPropertiesJson(relPropertyKeyMaps.at(labelIt->second));
        (*json)["Catalog"]["RelLabels"][label]["srcNodeLabels"] =
            getNodeLabelsString(relLabelToSrcNodeLabels[labelIt->second]);
        (*json)["Catalog"]["RelLabels"][label]["dstNodeLabels"] =
            getNodeLabelsString(relLabelToDstNodeLabels[labelIt->second]);
    }
    return json;
}

unique_ptr<nlohmann::json> Catalog::getPropertiesJson(
    const unordered_map<string, PropertyKey>& properties) {
    auto propertiesJson = make_unique<nlohmann::json>();
    for (unordered_map<string, PropertyKey>::const_iterator propIt = properties.begin();
         propIt != properties.end(); ++propIt) {
        nlohmann::json propertyJson =
            nlohmann::json{{"dataType", graphflow::common::DataTypeNames[propIt->second.dataType]},
                {"propIdx", to_string(propIt->second.idx)}};
        (*propertiesJson)[propIt->first] = propertyJson;
    }
    return propertiesJson;
}

string Catalog::getNodeLabelsString(vector<label_t> nodeLabels) {
    string nodeLabelsStr("");
    bool first = true;
    for (label_t nodeLabel : nodeLabels) {
        if (first) {
            first = false;
        } else {
            nodeLabelsStr += ", ";
        }
        nodeLabelsStr += getStringNodeLabel(nodeLabel);
    }
    return nodeLabelsStr;
}

} // namespace storage
} // namespace graphflow
