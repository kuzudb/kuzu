#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/serializer/writer.h"

namespace kuzu {
namespace common {

class Serializer {
public:
    explicit Serializer(std::shared_ptr<Writer> writer) : writer(std::move(writer)) {}

    template<typename T>
        requires std::is_trivially_destructible<T>::value || std::is_same<std::string, T>::value
    void serializeValue(const T& value) {
        writer->write((uint8_t*)&value, sizeof(T));
    }

    // Alias for serializeValue
    template<typename T>
    void write(const T& value) {
        serializeValue(value);
    }

    void write(const uint8_t* value, uint64_t len) { writer->write(value, len); }

    template<typename T>
    void serializeOptionalValue(const std::unique_ptr<T>& value) {
        serializeValue(value == nullptr);
        if (value != nullptr) {
            value->serialize(*this);
        }
    }

    template<typename T1, typename T2>
    void serializeUnorderedMap(const std::unordered_map<T1, std::unique_ptr<T2>>& values) {
        uint64_t mapSize = values.size();
        serializeValue(mapSize);
        for (auto& value : values) {
            serializeValue(value.first);
            value.second->serialize(*this);
        }
    }

    template<typename T>
    void serializeVector(const std::vector<T>& values) {
        uint64_t vectorSize = values.size();
        serializeValue<uint64_t>(vectorSize);
        for (auto& value : values) {
            if constexpr (requires(Serializer& ser) { value.serialize(ser); }) {
                value.serialize(*this);
            } else {
                serializeValue<T>(value);
            }
        }
    }

    template<typename T>
    void serializeVectorOfPtrs(const std::vector<std::unique_ptr<T>>& values) {
        uint64_t vectorSize = values.size();
        serializeValue<uint64_t>(vectorSize);
        for (auto& value : values) {
            value->serialize(*this);
        }
    }

    template<typename T>
    void serializeUnorderedSet(const std::unordered_set<T>& values) {
        uint64_t setSize = values.size();
        serializeValue(setSize);
        for (const auto& value : values) {
            serializeValue(value);
        }
    }

private:
    std::shared_ptr<Writer> writer;
};

template<>
void Serializer::serializeValue(const std::string& value);

} // namespace common
} // namespace kuzu
