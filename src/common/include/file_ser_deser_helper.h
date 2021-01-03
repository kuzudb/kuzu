#pragma once

#include <fcntl.h>
#include <stdio.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

using namespace std;

namespace graphflow {
namespace common {

class FileSerDeserHelper {

protected:
    FileSerDeserHelper(const string& fname, bool isSerializing);

    ~FileSerDeserHelper();

protected:
    FILE* f;
};

class FileSerHelper : public FileSerDeserHelper {

public:
    FileSerHelper(const string& fname) : FileSerDeserHelper(fname, true /* is serializing */) {}

    template<typename T>
    void write(const T& val) {
        fwrite(&val, sizeof(T), 1, f);
    }

    void writeString(const string& val);
};

class FileDeserHelper : public FileSerDeserHelper {

public:
    FileDeserHelper(const string& fname)
        : FileSerDeserHelper(fname, false /* is deserializing */) {}

    template<typename T>
    T read() {
        T val;
        if (1 != fread(&val, sizeof(T), 1, f)) {
            throw invalid_argument("Reading error inside FileDeserHelper#read(uint32_t& val).");
        }
        return val;
    }

    unique_ptr<string> readString();
};

} // namespace common
} // namespace graphflow