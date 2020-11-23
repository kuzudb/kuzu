#include "src/storage/include/structures/adj_lists.h"

#include <fstream>

#include "bitsery/adapter/stream.h"
#include "bitsery/brief_syntax.h"
#include "bitsery/traits/vector.h"

using OutputStreamAdapter = bitsery::Serializer<bitsery::OutputBufferedStreamAdapter>;

namespace graphflow {
namespace storage {

template<typename S>
void AdjListHeaders::serialize(S& s) {
    s.container(headers, UINT32_MAX, [](S& s, uint32_t& v) { s(v); });
}

void AdjListHeaders::saveToFile(const string& fname) {
    auto path = fname + ".headers";
    fstream f{path, f.binary | f.trunc | f.out};
    if (!f.is_open()) {
        invalid_argument("cannot open " + path + " for writing");
    }
    OutputStreamAdapter serializer{f};
    serializer.object(*this);
    serializer.adapter().flush();
    f.close();
}

void AdjListHeaders::readFromFile(const string& fname) {
    auto path = fname + ".headers";
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