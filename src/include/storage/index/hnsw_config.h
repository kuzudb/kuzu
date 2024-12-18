#pragma once

#include "common/types/types.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace storage {

enum class DistFuncType : uint8_t { Cosine = 0, L2 = 1, L2_SQUARE = 2, DotProduct = 3 };

// Max connectivity of the upper graph.
struct M {
    static constexpr const char* NAME = "m";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 30;

    static void validate(int64_t value);
};

// Max connectivity of the lower graph.
struct M0 {
    static constexpr const char* NAME = "m0";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 60;

    static void validate(int64_t value);
};

// Percentage of nodes in the upper layer.
struct Pu {
    static constexpr const char* NAME = "pu";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.05;

    static void validate(double value);
};

struct DistFunc {
    static constexpr const char* NAME = "distfunc";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static constexpr DistFuncType DEFAULT_VALUE = DistFuncType::Cosine;

    static void validate(const std::string& distFuncName);
};

struct Alpha {
    static constexpr const char* NAME = "alpha";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 1.1;

    static void validate(double value);
};

struct Efc {
    static constexpr const char* NAME = "efc";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 200;

    static void validate(int64_t value);
};

struct HNSWIndexConfig {
    int64_t connectivityInUpperLayer = M::DEFAULT_VALUE;
    int64_t connectivityInLowerLayer = M0::DEFAULT_VALUE;
    double upperLayerNodePct = Pu::DEFAULT_VALUE;
    DistFuncType distFunc = DistFunc::DEFAULT_VALUE;
    double alpha = Alpha::DEFAULT_VALUE;
    int64_t efc = Efc::DEFAULT_VALUE;

    HNSWIndexConfig() = default;
    explicit HNSWIndexConfig(const function::optional_params_t& optionalParams);

    void serialize(common::Serializer& ser) const;
    static HNSWIndexConfig deserialize(common::Deserializer& deSer);

private:
    static DistFuncType getDistFuncType(const std::string& funcName);
};

struct Efs {
    static constexpr const char* NAME = "efs";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 200;

    static void validate(int64_t value);
};

struct QueryHNSWConfig {
    int64_t efs = Efs::DEFAULT_VALUE;

    QueryHNSWConfig() = default;
    explicit QueryHNSWConfig(const function::optional_params_t& optionalParams);
};

} // namespace storage
} // namespace kuzu
