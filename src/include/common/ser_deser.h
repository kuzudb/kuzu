#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/file_utils.h"

namespace kuzu {
namespace common {

class SerDeser {

public:
    template<typename T>
    static uint64_t serializeValue(const T& value, FileInfo* fileInfo, uint64_t offset) {
        FileUtils::writeToFile(fileInfo, (uint8_t*)&value, sizeof(T), offset);
        return offset + sizeof(T);
    }

    template<typename T>
    static uint64_t deserializeValue(T& value, FileInfo* fileInfo, uint64_t offset) {
        FileUtils::readFromFile(fileInfo, (uint8_t*)&value, sizeof(T), offset);
        return offset + sizeof(T);
    }

    template<typename A, typename B>
    static uint64_t serializeUnorderedMap(
        const std::unordered_map<A, B>& values, FileInfo* fileInfo, uint64_t offset) {
        uint64_t mapSize = values.size();
        offset = serializeValue<uint64_t>(mapSize, fileInfo, offset);
        for (auto& value : values) {
            offset = serializeValue<A>(value.first, fileInfo, offset);
            offset = serializeValue<B>(value.second, fileInfo, offset);
        }
        return offset;
    }

    template<typename T>
    static uint64_t serializeVector(
        const std::vector<T>& values, FileInfo* fileInfo, uint64_t offset) {
        uint64_t vectorSize = values.size();
        offset = serializeValue<uint64_t>(vectorSize, fileInfo, offset);
        for (auto& value : values) {
            offset = serializeValue<T>(value, fileInfo, offset);
        }
        return offset;
    }

    template<typename A, typename B>
    static uint64_t deserializeUnorderedMap(
        std::unordered_map<A, B>& values, FileInfo* fileInfo, uint64_t offset) {
        uint64_t mapSize;
        offset = deserializeValue<uint64_t>(mapSize, fileInfo, offset);
        for (auto i = 0u; i < mapSize; i++) {
            uint64_t labelID;
            uint64_t numRels;
            offset = deserializeValue<A>(labelID, fileInfo, offset);
            offset = deserializeValue<B>(numRels, fileInfo, offset);
            values.emplace(labelID, numRels);
        }
        return offset;
    }

    template<typename T>
    static uint64_t deserializeVector(std::vector<T>& values, FileInfo* fileInfo, uint64_t offset) {
        uint64_t vectorSize;
        offset = deserializeValue<uint64_t>(vectorSize, fileInfo, offset);
        values.resize(vectorSize);
        for (auto& value : values) {
            offset = deserializeValue<T>(value, fileInfo, offset);
        }
        return offset;
    }

    template<typename T>
    static uint64_t serializeUnorderedSet(
        const std::unordered_set<T>& values, FileInfo* fileInfo, uint64_t offset) {
        uint64_t setSize = values.size();
        offset = serializeValue<uint64_t>(setSize, fileInfo, offset);
        for (auto& value : values) {
            offset = serializeValue<T>(value, fileInfo, offset);
        }
        return offset;
    }

    template<typename T>
    static uint64_t deserializeUnorderedSet(
        std::unordered_set<T>& values, FileInfo* fileInfo, uint64_t offset) {
        uint64_t setSize;
        offset = deserializeValue<uint64_t>(setSize, fileInfo, offset);
        for (auto i = 0u; i < setSize; i++) {
            T value;
            offset = deserializeValue<T>(value, fileInfo, offset);
            values.insert(value);
        }
        return offset;
    }
};

template<>
uint64_t SerDeser::serializeValue(const LogicalType& value, FileInfo* fileInfo, uint64_t offset);
template<>
uint64_t SerDeser::serializeValue(const std::string& value, FileInfo* fileInfo, uint64_t offset);

template<>
uint64_t SerDeser::deserializeValue(LogicalType& value, FileInfo* fileInfo, uint64_t offset);
template<>
uint64_t SerDeser::deserializeValue(std::string& value, FileInfo* fileInfo, uint64_t offset);

} // namespace common
} // namespace kuzu
