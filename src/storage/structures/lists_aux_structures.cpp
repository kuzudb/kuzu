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
    logger->trace("ListsMetadata: #Pages {}, #Chunks {}, #largeLists {}", numPages,
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

ListHeaders::ListHeaders(string path) : ListHeaders() {
    readFromFile(path);
    logger->trace("AdjListHeaders: #Headers {}", sizeof(headers.get()));
};

void ListHeaders::init(uint32_t size) {
    this->size = size;
    headers = make_unique<uint32_t[]>(size);
}

void ListHeaders::saveToFile(const string& fname) {
    auto path = fname + ".headers";
    if (0 == path.length()) {
        throw invalid_argument("ListHeaders: Empty filename");
    }
    uint32_t f = open(path.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1u == f) {
        throw invalid_argument("ListHeaders: Cannot create file: " + path);
    }
    auto bytesToWrite = size * sizeof(uint32_t);
    if (bytesToWrite != write(f, headers.get(), bytesToWrite)) {
        throw invalid_argument("ListHeaders: Cannot write in file.");
    }
    close(f);
}

void ListHeaders::readFromFile(const string& fname) {
    auto path = fname + ".headers";
    if (0 == path.length()) {
        throw invalid_argument("ListHeaders: Empty filename");
    }
    uint32_t f = open(path.c_str(), O_RDONLY);
    auto bytesToRead = lseek(f, 0, SEEK_END);
    size = bytesToRead / sizeof(uint32_t);
    headers = make_unique<uint32_t[]>(size);
    lseek(f, 0, SEEK_SET);
    if (bytesToRead != read(f, headers.get(), bytesToRead)) {
        throw invalid_argument("ListHeaders: Cannot read from file.");
    }
    close(f);
}

} // namespace storage
} // namespace graphflow
