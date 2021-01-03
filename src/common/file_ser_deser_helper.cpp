#include "src/common/include/file_ser_deser_helper.h"

namespace graphflow {

namespace common {

FileSerDeserHelper::FileSerDeserHelper(const string& fname, bool isSerializing) {
    f = isSerializing ? fopen(fname.c_str(), "w") : fopen(fname.c_str(), "r");
    if (nullptr == f) {
        throw invalid_argument("Cannot open file: " + fname);
    }
}

FileSerDeserHelper::~FileSerDeserHelper() {
    if (nullptr != f) {
        fclose(f);
    }
}

void FileSerHelper::writeString(const string& val) {
    uint64_t size = val.size();
    write(size);
    fwrite(val.data(), 1, size, f);
}

unique_ptr<string> FileDeserHelper::readString() {
    auto strVal = make_unique<string>();
    uint64_t size = read<uint64_t>();
    strVal->resize(size);
    fread(strVal->data(), 1, size, f);
    return strVal;
}

} // namespace common
} // namespace graphflow