#pragma once

#include <cmath>

#include "common/types/types.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace vector_extension {

enum class MetricType : uint8_t { Cosine = 0, L2 = 1, L2_SQUARE = 2, DotProduct = 3 };

// We use this ratio to calculate the max degree of the upper/lower graph based on the user provided
// max degree value for the upper/lower graph, respectively.
static constexpr double DEFAULT_DEGREE_THRESHOLD_RATIO = 1.25;

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

struct Metric {
    static constexpr const char* NAME = "metric";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static constexpr MetricType DEFAULT_VALUE = MetricType::Cosine;

    static void validate(const std::string& metric);
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

struct CacheEmbeddings {
    static constexpr const char* NAME = "cache_embeddings";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::BOOL;
    static constexpr bool DEFAULT_VALUE = true;
};

struct BlindSearchUpSelThreshold {
    static constexpr const char* NAME = "blind_search_up_sel";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.08;

    static void validate(double value);
};

struct DirectedSearchUpSelThreshold {
    static constexpr const char* NAME = "directed_search_up_sel";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::DOUBLE;
    static constexpr double DEFAULT_VALUE = 0.4;

    static void validate(double value);
};

struct HNSWIndexConfig {
    int64_t mu = Mu::DEFAULT_VALUE;
    int64_t ml = Ml::DEFAULT_VALUE;
    double pu = Pu::DEFAULT_VALUE;
    MetricType metric = Metric::DEFAULT_VALUE;
    double alpha = Alpha::DEFAULT_VALUE;
    int64_t efc = Efc::DEFAULT_VALUE;
    bool cacheEmbeddingsColumn = CacheEmbeddings::DEFAULT_VALUE;

    HNSWIndexConfig() = default;

    explicit HNSWIndexConfig(const function::optional_params_t& optionalParams);

    EXPLICIT_COPY_DEFAULT_MOVE(HNSWIndexConfig);

    void serialize(common::Serializer& ser) const;

    static HNSWIndexConfig deserialize(common::Deserializer& deSer);

    static std::string metricToString(MetricType metric);

private:
    HNSWIndexConfig(const HNSWIndexConfig& other)
        : mu{other.mu}, ml{other.ml}, pu{other.pu}, metric{other.metric}, alpha{other.alpha},
          efc{other.efc}, cacheEmbeddingsColumn(other.cacheEmbeddingsColumn) {}

    static MetricType getMetricType(const std::string& metricName);
};

struct QueryHNSWConfig {
    int64_t efs = Efs::DEFAULT_VALUE;
    double blindSearchUpSelThreshold = BlindSearchUpSelThreshold::DEFAULT_VALUE;
    double directedSearchUpSelThreshold = DirectedSearchUpSelThreshold::DEFAULT_VALUE;

    QueryHNSWConfig() = default;

    explicit QueryHNSWConfig(const function::optional_params_t& optionalParams);
};

} // namespace vector_extension
} // namespace kuzu
