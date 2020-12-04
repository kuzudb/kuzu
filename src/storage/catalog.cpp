#include "src/storage/include/catalog.h"

#include <fstream>
#include <iostream>

#include "bitsery/adapter/stream.h"
#include "bitsery/bitsery.h"
#include "bitsery/ext/std_map.h"
#include "bitsery/traits/string.h"
#include "bitsery/traits/vector.h"
#include <bitsery/brief_syntax.h>
#include <bitsery/brief_syntax/string.h>
#include <bitsery/ext/pointer.h>
#include <bitsery/ext/std_map.h>

using namespace std;
using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

const vector<label_t>& Catalog::getRelLabelsForNodeLabelDirection(
    label_t nodeLabel, Direction direction) const {
    if (nodeLabel >= getNodeLabelsCount()) {
        throw invalid_argument("Node label out of the bounds.");
    }
    if (FWD == direction) {
        return srcNodeLabelToRelLabel[nodeLabel];
    }
    return dstNodeLabelToRelLabel[nodeLabel];
}

const vector<label_t>& Catalog::getNodeLabelsForRelLabelDir(
    label_t relLabel, Direction direction) const {
    if (relLabel >= getRelLabelsCount()) {
        throw invalid_argument("Node label out of the bounds.");
    }
    if (FWD == direction) {
        return relLabelToSrcNodeLabels[relLabel];
    }
    return relLabelToDstNodeLabels[relLabel];
}

template<typename S>
void Catalog::serialize(S& s) {
    auto vetorPropertyFunc = [](S& s, vector<Property>& v) {
        s.container(v, UINT32_MAX, [](S& s, Property& w) { s(w.name, w.dataType); });
    };
    s.container(nodePropertyMaps, UINT32_MAX, vetorPropertyFunc);
    s.container(relPropertyMaps, UINT32_MAX, vetorPropertyFunc);

    auto vectorLabelsFunc = [](S& s, vector<label_t>& v) {
        s.container(v, UINT32_MAX, [](S& s, label_t& w) { s(w); });
    };
    s.container(relLabelToSrcNodeLabels, UINT32_MAX, vectorLabelsFunc);
    s.container(relLabelToDstNodeLabels, UINT32_MAX, vectorLabelsFunc);
    s.container(srcNodeLabelToRelLabel, UINT32_MAX, vectorLabelsFunc);
    s.container(dstNodeLabelToRelLabel, UINT32_MAX, vectorLabelsFunc);

    s.container(relLabelToCardinalityMap, UINT32_MAX, [](S& s, Cardinality& v) { s(v); });
}

void Catalog::saveToFile(const string& directory) {
    auto path = directory + "/catalog.bin";
    fstream f{path, f.binary | f.trunc | f.out};
    if (!f.is_open()) {
        invalid_argument("cannot open " + path + " for writing");
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
    fstream f{path, f.binary | f.in};
    if (!f.is_open()) {
        invalid_argument("Cannot open " + path + " for reading the catalog.");
    }
    deserializeStringToLabelMap(f, stringToNodeLabelMap);
    deserializeStringToLabelMap(f, stringToRelLabelMap);
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (state.first == bitsery::ReaderError::NoError && state.second) {
        invalid_argument("Cannot deserialize the catalog.");
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

} // namespace storage
} // namespace graphflow
