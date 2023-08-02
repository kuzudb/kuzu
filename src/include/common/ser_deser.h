#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/file_utils.h"

namespace kuzu {
namespace common {

class LogicalType;

class SerDeser {

public:
    template<typename T>
    static void serializeValue(const T& value, FileInfo* fileInfo, uint64_t& offset) {
        static_assert(std::is_trivially_destructible<T>(), "value must be a trivial type");
        FileUtils::writeToFile(fileInfo, (uint8_t*)&value, sizeof(T), offset);
        offset += sizeof(T);
    }

    template<typename T>
    static void serializeOptionalValue(
        const std::unique_ptr<T>& value, FileInfo* fileInfo, uint64_t& offset) {
        serializeValue(value == nullptr, fileInfo, offset);
        if (value != nullptr) {
            value->serialize(fileInfo, offset);
        }
    }

    template<typename T>
    static void deserializeValue(T& value, FileInfo* fileInfo, uint64_t& offset) {
        static_assert(std::is_trivially_destructible<T>(), "value must be a trivial type");
        FileUtils::readFromFile(fileInfo, (uint8_t*)&value, sizeof(T), offset);
        offset += sizeof(T);
    }

    template<typename T>
    static void deserializeOptionalValue(
        std::unique_ptr<T>& value, FileInfo* fileInfo, uint64_t& offset) {
        bool isNull;
        deserializeValue(isNull, fileInfo, offset);
        if (!isNull) {
            value = T::deserialize(fileInfo, offset);
        }
    }

    template<typename T1, typename T2>
    static void serializeUnorderedMap(const std::unordered_map<T1, std::unique_ptr<T2>>& values,
        FileInfo* fileInfo, uint64_t& offset) {
        uint64_t mapSize = values.size();
        serializeValue(mapSize, fileInfo, offset);
        for (auto& value : values) {
            serializeValue(value.first, fileInfo, offset);
            value.second->serialize(fileInfo, offset);
        }
    }

    template<typename T>
    static void serializeVector(
        const std::vector<T>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t vectorSize = values.size();
        serializeValue<uint64_t>(vectorSize, fileInfo, offset);
        for (auto& value : values) {
            serializeValue<T>(value, fileInfo, offset);
        }
    }

    template<typename T>
    static void serializeVectorOfPtrs(
        const std::vector<std::unique_ptr<T>>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t vectorSize = values.size();
        serializeValue<uint64_t>(vectorSize, fileInfo, offset);
        for (auto& value : values) {
            value->serialize(fileInfo, offset);
        }
    }

    template<typename T1, typename T2>
    static void deserializeUnorderedMap(
        std::unordered_map<T1, std::unique_ptr<T2>>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t mapSize;
        deserializeValue<uint64_t>(mapSize, fileInfo, offset);
        values.reserve(mapSize);
        for (auto i = 0u; i < mapSize; i++) {
            T1 key;
            deserializeValue<T1>(key, fileInfo, offset);
            auto val = T2::deserialize(fileInfo, offset);
            values.emplace(key, std::move(val));
        }
    }

    template<typename T>
    static void deserializeVector(std::vector<T>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t vectorSize;
        deserializeValue(vectorSize, fileInfo, offset);
        values.resize(vectorSize);
        for (auto& value : values) {
            deserializeValue(value, fileInfo, offset);
        }
    }

    template<typename T>
    static void deserializeVectorOfPtrs(
        std::vector<std::unique_ptr<T>>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t vectorSize;
        deserializeValue(vectorSize, fileInfo, offset);
        values.reserve(vectorSize);
        for (auto i = 0u; i < vectorSize; i++) {
            values.push_back(T::deserialize(fileInfo, offset));
        }
    }

    template<typename T>
    static void serializeUnorderedSet(
        const std::unordered_set<T>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t setSize = values.size();
        serializeValue(setSize, fileInfo, offset);
        for (auto& value : values) {
            serializeValue(value, fileInfo, offset);
        }
    }

    template<typename T>
    static void deserializeUnorderedSet(
        std::unordered_set<T>& values, FileInfo* fileInfo, uint64_t& offset) {
        uint64_t setSize;
        deserializeValue(setSize, fileInfo, offset);
        for (auto i = 0u; i < setSize; i++) {
            T value;
            deserializeValue<T>(value, fileInfo, offset);
            values.insert(value);
        }
    }
};

template<>
void SerDeser::serializeValue(const std::string& value, FileInfo* fileInfo, uint64_t& offset);

template<>
void SerDeser::deserializeValue(std::string& value, FileInfo* fileInfo, uint64_t& offset);

} // namespace common
} // namespace kuzu
