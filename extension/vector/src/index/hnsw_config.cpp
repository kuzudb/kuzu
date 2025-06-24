#include "index/hnsw_config.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"
#include "function/hnsw_index_functions.h"

namespace kuzu {
namespace vector_extension {

// The maximum allowed degree to be defined by users.
static constexpr int64_t MAX_DEGREE =
    static_cast<int64_t>(std::numeric_limits<int16_t>::max() / DEFAULT_DEGREE_THRESHOLD_RATIO);

void Mu::validate(int64_t value) {
    if (value < 1 || value > MAX_DEGREE) {
        throw common::BinderException{
            common::stringFormat("Mu must be a positive integer between 1 and {}.", MAX_DEGREE)};
    }
}

void Ml::validate(int64_t value) {
    if (value < 1 || value > MAX_DEGREE) {
        throw common::BinderException{
            common::stringFormat("Ml must be a positive integer between 1 and {}.", MAX_DEGREE)};
    }
    if (HNSWIndex::getDegreeThresholdToShrink(value) >
        static_cast<int64_t>(common::DEFAULT_VECTOR_CAPACITY)) {
        throw common::BinderException(common::stringFormat(
            "Unsupported configured ml value {}, the maximum supported value is {}.", value,
            HNSWIndex::getMaximumSupportedMl()));
    }
}

void Pu::validate(double value) {
    if (value < 0 || value > 1) {
        throw common::BinderException{"Pu must be a double between 0 and 1."};
    }
}

void Metric::validate(const std::string& metric) {
    const auto lowerCaseMetric = common::StringUtils::getLower(metric);
    if (lowerCaseMetric != "cosine" && lowerCaseMetric != "l2" && lowerCaseMetric != "l2sq" &&
        lowerCaseMetric != "dotproduct" && lowerCaseMetric != "dot_product") {
        throw common::BinderException{"Metric must be one of COSINE, L2, L2SQ or DOTPRODUCT."};
    }
}

void Efc::validate(int64_t value) {
    if (value < 1) {
        throw common::BinderException{"Efc must be a positive integer."};
    }
}

void Alpha::validate(double value) {
    if (value < 1) {
        throw common::BinderException{"Alpha must be a double greater than or equal to 1."};
    }
}

void Efs::validate(int64_t value) {
    if (value < 1) {
        throw common::BinderException{"Efs must be a positive integer."};
    }
}

void BlindSearchUpSelThreshold::validate(double value) {
    if (value < 0 || value > 1) {
        throw common::BinderException{
            "Blind search upper selectivity threshold must be a double between 0 and 1."};
    }
}

void DirectedSearchUpSelThreshold::validate(double value) {
    if (value < 0 || value > 1) {
        throw common::BinderException{
            "Directed search upper selectivity threshold must be a double between 0 and 1."};
    }
}

HNSWIndexConfig::HNSWIndexConfig(const function::optional_params_t& optionalParams) {
    for (auto& [name, value] : optionalParams) {
        auto lowerCaseName = common::StringUtils::getLower(name);
        if (Mu::NAME == lowerCaseName) {
            value.validateType(Mu::TYPE);
            mu = value.getValue<int64_t>();
            Mu::validate(mu);
        } else if (Ml::NAME == lowerCaseName) {
            value.validateType(Ml::TYPE);
            ml = value.getValue<int64_t>();
            Ml::validate(ml);
        } else if (Pu::NAME == lowerCaseName) {
            value.validateType(Pu::TYPE);
            pu = value.getValue<double>();
            Pu::validate(pu);
        } else if (Metric::NAME == lowerCaseName) {
            value.validateType(Metric::TYPE);
            auto funcName = value.getValue<std::string>();
            Metric::validate(funcName);
            metric = getMetricType(funcName);
        } else if (Alpha::NAME == lowerCaseName) {
            value.validateType(Alpha::TYPE);
            alpha = value.getValue<double>();
            Alpha::validate(alpha);
        } else if (Efc::NAME == lowerCaseName) {
            value.validateType(Efc::TYPE);
            efc = value.getValue<int64_t>();
            Efc::validate(efc);
        } else if (CacheEmbeddings::NAME == lowerCaseName) {
            value.validateType(CacheEmbeddings::TYPE);
            cacheEmbeddingsColumn = value.getValue<bool>();
        } else {
            throw common::BinderException{
                common::stringFormat("Unrecognized optional parameter {} in {}.", name,
                    CreateVectorIndexFunction::name)};
        }
    }
}

std::string HNSWIndexConfig::metricToString(MetricType metric) {
    switch (metric) {
    case MetricType::Cosine: {
        return "cosine";
    }
    case MetricType::L2: {
        return "l2";
    }
    case MetricType::L2_SQUARE: {
        return "l2sq";
    }
    case MetricType::DotProduct: {
        return "dotproduct";
    }
    default: {
        throw common::RuntimeException(common::stringFormat("Unknown distance function type {}.",
            static_cast<int64_t>(metric)));
    }
    }
}

void HNSWIndexConfig::serialize(common::Serializer& ser) const {
    ser.writeDebuggingInfo("degreeInUpperLayer");
    ser.serializeValue(mu);
    ser.writeDebuggingInfo("degreeInLowerLayer");
    ser.serializeValue(ml);
    ser.writeDebuggingInfo("metric");
    ser.serializeValue<uint8_t>(static_cast<uint8_t>(metric));
    ser.writeDebuggingInfo("alpha");
    ser.serializeValue(alpha);
    ser.writeDebuggingInfo("efc");
    ser.serializeValue(efc);
}

HNSWIndexConfig HNSWIndexConfig::deserialize(common::Deserializer& deSer) {
    auto config = HNSWIndexConfig{};
    std::string debuggingInfo;
    deSer.validateDebuggingInfo(debuggingInfo, "degreeInUpperLayer");
    deSer.deserializeValue(config.mu);
    deSer.validateDebuggingInfo(debuggingInfo, "degreeInLowerLayer");
    deSer.deserializeValue(config.ml);
    deSer.validateDebuggingInfo(debuggingInfo, "metric");
    uint8_t metric = 0;
    deSer.deserializeValue(metric);
    config.metric = static_cast<MetricType>(metric);
    deSer.validateDebuggingInfo(debuggingInfo, "alpha");
    deSer.deserializeValue(config.alpha);
    deSer.validateDebuggingInfo(debuggingInfo, "efc");
    deSer.deserializeValue(config.efc);
    return config;
}

MetricType HNSWIndexConfig::getMetricType(const std::string& metricName) {
    const auto lowerMetricName = common::StringUtils::getLower(metricName);
    if (lowerMetricName == "cosine") {
        return MetricType::Cosine;
    }
    if (lowerMetricName == "l2") {
        return MetricType::L2;
    }
    if (lowerMetricName == "l2sq") {
        return MetricType::L2_SQUARE;
    }
    if (lowerMetricName == "dot_product" || lowerMetricName == "dotproduct") {
        return MetricType::DotProduct;
    }
    KU_UNREACHABLE;
}

QueryHNSWConfig::QueryHNSWConfig(const function::optional_params_t& optionalParams) {
    for (auto& [name, value] : optionalParams) {
        auto lowerCaseName = common::StringUtils::getLower(name);
        if (Efs::NAME == lowerCaseName) {
            value.validateType(Efs::TYPE);
            efs = value.getValue<int64_t>();
            Efs::validate(efs);
        } else if (BlindSearchUpSelThreshold::NAME == lowerCaseName) {
            value.validateType(BlindSearchUpSelThreshold::TYPE);
            blindSearchUpSelThreshold = value.getValue<double>();
            BlindSearchUpSelThreshold::validate(blindSearchUpSelThreshold);
        } else if (DirectedSearchUpSelThreshold::NAME == lowerCaseName) {
            value.validateType(DirectedSearchUpSelThreshold::TYPE);
            directedSearchUpSelThreshold = value.getValue<double>();
            DirectedSearchUpSelThreshold::validate(directedSearchUpSelThreshold);
        } else {
            throw common::BinderException{common::stringFormat(
                "Unrecognized optional parameter {} in {}.", name, QueryVectorIndexFunction::name)};
        }
    }
    if (blindSearchUpSelThreshold >= directedSearchUpSelThreshold) {
        throw common::BinderException{common::stringFormat(
            "Blind search upper selectivity threshold is set to {}, but the directed search upper "
            "selectivity threshold is set to {}. The blind search upper selectivity threshold must "
            "be less than the directed search upper selectivity threshold.",
            blindSearchUpSelThreshold, directedSearchUpSelThreshold)};
    }
}

} // namespace vector_extension
} // namespace kuzu
