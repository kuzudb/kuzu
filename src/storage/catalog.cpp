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
#include <bitsery/ext/std_map.h>

using namespace std;
using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

const vector<gfLabel_t> &Catalog::getRelLabelsForNodeLabelDirection(
    gfLabel_t nodeLabel, Direction direction) const{
    if (nodeLabel >= getNodeLabelsCount()) {
        throw invalid_argument("Node label out of the bounds.");
    }
    if (FORWARD == direction) {
        return srcNodeLabelToRelLabel[nodeLabel];
    }
    return dstNodeLabelToRelLabel[nodeLabel];
}

const vector<gfLabel_t> &Catalog::getNodeLabelsForRelLabelDir(
    gfLabel_t relLabel, Direction direction) const{
    if (relLabel >= getRelLabelsCount()) {
        throw invalid_argument("Node label out of the bounds.");
    }
    if (FORWARD == direction) {
        return relLabelToSrcNodeLabels[relLabel];
    }
    return relLabelToDstNodeLabels[relLabel];
}

template<typename S>
void Catalog::serialize(S &s) {
    auto stringToLabelMapFunc = [](S &s, string &key, gfLabel_t &value) { s(key, value); };
    s.ext(stringToNodeLabelMap, bitsery::ext::StdMap{UINT32_MAX}, stringToLabelMapFunc);
    s.ext(stringToRelLabelMap, bitsery::ext::StdMap{UINT32_MAX}, stringToLabelMapFunc);

    auto vetorPropertyFunc = [](S &s, vector<Property> &v) {
        s.container(v, UINT32_MAX, [](S &s, Property &w) { s(w.propertyName, w.dataType); });
    };
    s.container(nodePropertyMaps, UINT32_MAX, vetorPropertyFunc);
    s.container(relPropertyMaps, UINT32_MAX, vetorPropertyFunc);

    auto vectorLabelsFunc = [](S &s, vector<gfLabel_t> &v) {
        s.container(v, UINT32_MAX, [](S &s, gfLabel_t &w) { s(w); });
    };
    s.container(relLabelToSrcNodeLabels, UINT32_MAX, vectorLabelsFunc);
    s.container(relLabelToDstNodeLabels, UINT32_MAX, vectorLabelsFunc);
    s.container(srcNodeLabelToRelLabel, UINT32_MAX, vectorLabelsFunc);
    s.container(dstNodeLabelToRelLabel, UINT32_MAX, vectorLabelsFunc);
    
    s.container(relLabelToCardinalityMap, UINT32_MAX, [](S &s, Cardinality &v) { s(v); });
}

void Catalog::saveToFile(const string &directory) {
    auto path = directory + "/catalog.bin";
    fstream f{path, f.binary | f.trunc | f.out};
    if (!f.is_open()) {
        invalid_argument("cannot open " + path + " for writing");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void Catalog::readFromFile(const string &directory) {
    auto path = directory + "/catalog.bin";
    fstream f{path, f.binary | f.in};
    if (!f.is_open()) {
        invalid_argument("Cannot open " + path + " for reading the catalog.");
    }
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (state.first == bitsery::ReaderError::NoError && state.second) {
        invalid_argument("Cannot deserialize the catalog.");
    }
}

} // namespace storage
} // namespace graphflow
