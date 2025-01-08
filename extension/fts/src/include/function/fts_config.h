#pragma once

#include <string>

#include "common/types/types.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace fts_extension {

struct Stemmer {
    static constexpr const char* NAME = "stemmer";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static constexpr const char* DEFAULT_VALUE = "english";

    static void validate(const std::string& stemmer);
};

struct FTSConfig {
    std::string stemmer = Stemmer::DEFAULT_VALUE;

    FTSConfig() = default;
    explicit FTSConfig(const function::optional_params_t& optionalParams);

    void serialize(common::Serializer& serializer) const;

    static FTSConfig deserialize(common::Deserializer& deserializer);

    uint64_t getNumBytesForSerialization() const;
};

struct K {
    static constexpr const char* NAME = "k";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 1.2;
    static void validate(double value);
};

struct B {
    static constexpr const char* NAME = "b";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.75;
    static void validate(double value);
};

struct Conjunctive {
    static constexpr const char* NAME = "conjunctive";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::BOOL;
    static constexpr bool DEFAULT_VALUE = false;
};

struct QueryFTSConfig {
    // k: parameter controls the influence of term frequency saturation. It limits the effect of
    // additional occurrences of a term within a document.
    double k = K::DEFAULT_VALUE;
    // b: parameter controls the degree of length normalization by adjusting the influence of
    // document length.
    double b = B::DEFAULT_VALUE;
    bool isConjunctive = Conjunctive::DEFAULT_VALUE;

    QueryFTSConfig() = default;
    explicit QueryFTSConfig(const function::optional_params_t& optionalParams);
};

} // namespace fts_extension
} // namespace kuzu
