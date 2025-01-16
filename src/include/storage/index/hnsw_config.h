#pragma once

#include "common/types/types.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace storage {

enum class DistFuncType : uint8_t { Cosine = 0, L2 = 1, L2_SQUARE = 2, DotProduct = 3 };

// Max degree of the upper graph.
struct Mu {
    static constexpr const char* NAME = "mu";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 30;

    static void validate(int64_t value);
};

// Max degree of the lower graph.
struct Ml {
    static constexpr const char* NAME = "ml";
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

struct Efs {
    static constexpr const char* NAME = "efs";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::INT64;
    static constexpr int64_t DEFAULT_VALUE = 200;

    static void validate(int64_t value);
};

struct HNSWIndexConfig {
    int64_t mu = Mu::DEFAULT_VALUE;
    int64_t ml = Ml::DEFAULT_VALUE;
    double pu = Pu::DEFAULT_VALUE;
    DistFuncType distFunc = DistFunc::DEFAULT_VALUE;
    double alpha = Alpha::DEFAULT_VALUE;
    int64_t efc = Efc::DEFAULT_VALUE;

    HNSWIndexConfig() = default;

    explicit HNSWIndexConfig(const function::optional_params_t& optionalParams);

    EXPLICIT_COPY_DEFAULT_MOVE(HNSWIndexConfig);

    void serialize(common::Serializer& ser) const;

    static HNSWIndexConfig deserialize(common::Deserializer& deSer);

    static std::string distFuncToString(DistFuncType distFunc);

private:
    HNSWIndexConfig(const HNSWIndexConfig& other)
        : mu{other.mu}, ml{other.ml}, pu{other.pu}, distFunc{other.distFunc}, alpha{other.alpha},
          efc{other.efc} {}

    static DistFuncType getDistFuncType(const std::string& funcName);
};

struct QueryHNSWConfig {
    int64_t efs = Efs::DEFAULT_VALUE;

    QueryHNSWConfig() = default;

    explicit QueryHNSWConfig(const function::optional_params_t& optionalParams);
};

} // namespace storage
} // namespace kuzu
