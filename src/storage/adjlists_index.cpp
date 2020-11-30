#include "src/storage/include/adjlists_index.h"

#include <fstream>
#include <iostream>

#include "bitsery/adapter/stream.h"
#include "bitsery/traits/vector.h"
#include <bitsery/brief_syntax.h>

using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

template<typename S>
void AdjListsMetadata::serialize(S& s) {
    s.value8b(numPages);
    s.container(headers, UINT32_MAX, [](S& s, uint32_t& v) { s(v); });

    auto vetorUINT64Func = [](S& s, vector<uint64_t>& v) {
        s.container(v, UINT32_MAX, [](S& s, uint64_t& w) { s(w); });
    };
    s.container(chunksPagesMap, UINT32_MAX, vetorUINT64Func);
    s.container(lAdjListsPagesMap, UINT32_MAX, vetorUINT64Func);
}

void AdjListsMetadata::saveToFile(const string& fname) {
    auto path = fname + ".metadata";
    fstream f{path, f.binary | f.trunc | f.out};
    if (!f.is_open()) {
        invalid_argument("cannot open " + path + " for writing");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void AdjListsMetadata::readFromFile(const string& fname) {
    auto path = fname + ".metadata";
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
