#include "src/storage/include/structures/lists_aux_structures.h"

#include <fstream>

#include "bitsery/adapter/stream.h"
#include "bitsery/brief_syntax.h"
#include "bitsery/traits/vector.h"

using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

ListsMetadata::ListsMetadata(const string& path) : ListsMetadata() {
    readFromFile(path);
    logger->trace("ListsMetadata: #Pages {}, #Chunks {}, #lAdjLists {}", numPages,
        chunksPagesMap.size(), largeListsPagesMap.size());
};

template<typename S>
void ListsMetadata::serialize(S& s) {
    s.value8b(numPages);
    auto vetorUINT64Func = [](S& s, vector<uint64_t>& v) {
        s.container(v, UINT32_MAX, [](S& s, uint64_t& w) { s(w); });
    };
    s.container(chunksPagesMap, UINT32_MAX, vetorUINT64Func);
    s.container(largeListsPagesMap, UINT32_MAX, vetorUINT64Func);
}

void ListsMetadata::saveToFile(const string& fname) {
    auto path = fname + ".metadata";
    fstream f{path, f.binary | f.trunc | f.out};
    if (f.fail()) {
        throw invalid_argument("cannot open " + path + " for writing");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void ListsMetadata::readFromFile(const string& fname) {
    auto path = fname + ".metadata";
    logger->trace("ListsMetadata: Path {}", path);
    fstream f{path, f.binary | f.in};
    if (f.fail()) {
        throw invalid_argument("Cannot open " + path + " for reading the catalog.");
    }
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (!(state.first == bitsery::ReaderError::NoError && state.second)) {
        throw invalid_argument("Cannot deserialize the ListsMetadata.");
    }
}

AdjListHeaders::AdjListHeaders(string path) : AdjListHeaders() {
    readFromFile(path);
    logger->trace("AdjListHeaders: #Headers {}", headers.size());
};

template<typename S>
void AdjListHeaders::serialize(S& s) {
    s.container(headers, UINT32_MAX, [](S& s, uint32_t& v) { s(v); });
}

void AdjListHeaders::saveToFile(const string& fname) {
    auto path = fname + ".headers";
    fstream f{path, f.binary | f.trunc | f.out};
    if (f.fail()) {
        throw invalid_argument("cannot open " + path + " for writing");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void AdjListHeaders::readFromFile(const string& fname) {
    auto path = fname + ".headers";
    logger->trace("AdjListHeaders: Path {}", path);
    fstream f{path, f.binary | f.in};
    if (f.fail()) {
        throw invalid_argument("Cannot open " + path + " for reading the catalog.");
    }
    auto state = bitsery::quickDeserialization<bitsery::InputStreamAdapter>(f, *this);
    f.close();
    if (!(state.first == bitsery::ReaderError::NoError && state.second)) {
        throw invalid_argument("Cannot deserialize the AdjListHeaders.");
    }
}

} // namespace storage
} // namespace graphflow
