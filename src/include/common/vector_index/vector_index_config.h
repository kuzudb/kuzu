

#pragma once

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace common {

enum DistanceFunc {
    L2 = 0,
    COSINE = 1,
    IP = 2,
};

struct VectorIndexConfig {
    constexpr static const char* MAX_NBRS_AT_UPPER_LEVEL = "MAXNBRSATUPPERLEVEL";
    constexpr static const char* MAX_NBRS_AT_LOWER_LEVEL = "MAXNBRSATLOWERLEVEL";
    constexpr static const char* SAMPLING_PROBABILITY = "SAMPLINGPROBABILITY";
    constexpr static const char* EF_CONSTRUCTION = "EFCONSTRUCTION";
    constexpr static const char* EF_SEARCH = "EFSEARCH";
    constexpr static const char* ALPHA = "ALPHA";
    constexpr static const char* NUM_VECTORS_PER_PARTITION = "NUMVECTORSPERPARTITION";
    constexpr static const char* SQ_ENABLED = "SQENABLED";
    constexpr static const char* DISTANCE_FUNC = "DISTANCEFUNC";

    // The maximum number of neighbors to keep for each node at the upper level
    int maxNbrsAtUpperLevel = 64;

    // The maximum number of neighbors to keep for each node at the lower level
    int maxNbrsAtLowerLevel = 128;

    // Sampling probability for the upper level
    float samplingProbability = 0.05;

    // The number of neighbors to consider during construction
    int efConstruction = 200;

    // The number of neighbors to consider during search
    int efSearch = 200;

    // The alpha parameter for the RNG heuristic
    float alpha = 1.0;

    // The number of node groups per partition (default to 5M)
    int numberVectorsPerPartition = 5000000;

    // Whether scalar quantization is enabled
    bool sqEnabled = true;

    // The distance function to use
    DistanceFunc distanceFunc = DistanceFunc::COSINE;

    VectorIndexConfig() = default;

    static VectorIndexConfig construct(
        const std::unordered_map<std::string, common::Value>& options) {
        VectorIndexConfig config;
        for (const auto& [key, value] : options) {
            if (key == MAX_NBRS_AT_UPPER_LEVEL) {
                config.maxNbrsAtUpperLevel = value.getValue<int64_t>();
            } else if (key == MAX_NBRS_AT_LOWER_LEVEL) {
                config.maxNbrsAtLowerLevel = value.getValue<int64_t>();
            } else if (key == SAMPLING_PROBABILITY) {
                config.samplingProbability = value.getValue<double>();
                KU_ASSERT_MSG(config.samplingProbability >= 0.0 &&
                                  config.samplingProbability <= 0.4,
                    "Sampling percentage should be less than 40%");
            } else if (key == EF_CONSTRUCTION) {
                config.efConstruction = value.getValue<int64_t>();
            } else if (key == EF_SEARCH) {
                config.efSearch = value.getValue<int64_t>();
            } else if (key == ALPHA) {
                config.alpha = value.getValue<double>();
            } else if (key == NUM_VECTORS_PER_PARTITION) {
                config.numberVectorsPerPartition = value.getValue<int64_t>();
            } else if (key == SQ_ENABLED) {
                config.sqEnabled = value.getValue<bool>();
            } else if (key == DISTANCE_FUNC) {
                auto distFunc = value.toString();
                if (distFunc == "L2") {
                    config.distanceFunc = DistanceFunc::L2;
                } else if (distFunc == "COSINE") {
                    config.distanceFunc = DistanceFunc::COSINE;
                } else if (distFunc == "IP") {
                    config.distanceFunc = DistanceFunc::IP;
                } else {
                    KU_ASSERT(false);
                }
            } else {
                KU_ASSERT(false);
            }
        }
        return config;
    }

    void serialize(Serializer& serializer) const {
        serializer.serializeValue(maxNbrsAtUpperLevel);
        serializer.serializeValue(maxNbrsAtLowerLevel);
        serializer.serializeValue(samplingProbability);
        serializer.serializeValue(efConstruction);
        serializer.serializeValue(efSearch);
        serializer.serializeValue(alpha);
        serializer.serializeValue(numberVectorsPerPartition);
        serializer.serializeValue(sqEnabled);
        serializer.serializeValue(static_cast<int>(distanceFunc));
    }

    static VectorIndexConfig deserialize(Deserializer& deserializer) {
        VectorIndexConfig config;
        deserializer.deserializeValue(config.maxNbrsAtUpperLevel);
        deserializer.deserializeValue(config.maxNbrsAtLowerLevel);
        deserializer.deserializeValue(config.samplingProbability);
        deserializer.deserializeValue(config.efConstruction);
        deserializer.deserializeValue(config.efSearch);
        deserializer.deserializeValue(config.alpha);
        deserializer.deserializeValue(config.numberVectorsPerPartition);
        deserializer.deserializeValue(config.sqEnabled);
        int distanceFunc;
        deserializer.deserializeValue(distanceFunc);
        config.distanceFunc = static_cast<DistanceFunc>(distanceFunc);
        return config;
    }
};

} // namespace common
} // namespace kuzu
