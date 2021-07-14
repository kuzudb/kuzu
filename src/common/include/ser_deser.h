#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "src/common/include/file_utils.h"

using namespace std;

namespace graphflow {
namespace common {

class SerDeser {

public:
    template<typename T>
    static uint64_t serializeValue(const T& value, int fd, uint64_t offset) {
        FileUtils::writeToFile(fd, (uint8_t*)&value, sizeof(T), offset);
        return offset + sizeof(T);
    }

    template<typename T>
    static uint64_t deserializeValue(T& value, int fd, uint64_t offset) {
        FileUtils::readFromFile(fd, (uint8_t*)&value, sizeof(T), offset);
        return offset + sizeof(T);
    }

    template<typename T>
    static uint64_t serializeVector(const vector<T>& values, int fd, uint64_t offset) {
        uint64_t vectorSize = values.size();
        offset = serializeValue<uint64_t>(vectorSize, fd, offset);
        for (auto& value : values) {
            offset = serializeValue<T>(value, fd, offset);
        }
        return offset;
    }

    template<typename T>
    static uint64_t deserializeVector(vector<T>& values, int fd, uint64_t offset) {
        uint64_t vectorSize;
        offset = deserializeValue<uint64_t>(vectorSize, fd, offset);
        values.resize(vectorSize);
        for (auto& value : values) {
            offset = deserializeValue<T>(value, fd, offset);
        }
        return offset;
    }

    template<typename T>
    static uint64_t serializeUnorderedSet(const unordered_set<T>& values, int fd, uint64_t offset) {
        uint64_t setSize = values.size();
        offset = serializeValue<uint64_t>(setSize, fd, offset);
        for (auto& value : values) {
            offset = serializeValue<T>(value, fd, offset);
        }
        return offset;
    }

    template<typename T>
    static uint64_t deserializeUnorderedSet(unordered_set<T>& values, int fd, uint64_t offset) {
        uint64_t setSize;
        offset = deserializeValue<uint64_t>(setSize, fd, offset);
        for (auto i = 0u; i < setSize; i++) {
            T value;
            offset = deserializeValue<T>(value, fd, offset);
            values.insert(value);
        }
        return offset;
    }
};

} // namespace common
} // namespace graphflow
