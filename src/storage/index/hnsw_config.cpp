#include "storage/index/hnsw_config.h"

#include "common/exception/binder.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"
#include "function/table/hnsw/hnsw_index_functions.h"

namespace kuzu {
namespace storage {

void Mu::validate(int64_t value) {
    if (value < 1 || value > 100) {
        throw common::BinderException{"Mu must be a positive integer between 1 and 100."};
    }
}

void Ml::validate(int64_t value) {
    if (value < 1 || value > 200) {
        throw common::BinderException{"Ml must be a positive integer between 1 and 200."};
    }
}

void Pu::validate(double value) {
    if (value < 0 || value > 1) {
        throw common::BinderException{"Pu must be a double between 0 and 1."};
    }
}

void DistFunc::validate(const std::string& distFuncName) {
    const auto lowerCaseFuncName = common::StringUtils::getLower(distFuncName);
    if (lowerCaseFuncName != "cosine" && lowerCaseFuncName != "l2" && lowerCaseFuncName != "l2sq" &&
        lowerCaseFuncName != "dotproduct") {
        throw common::BinderException{"DistFunc must be one of COSINE, L2, L2SQ or DOTPRODUCT."};
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
        } else if (DistFunc::NAME == lowerCaseName) {
            value.validateType(DistFunc::TYPE);
            auto funcName = value.getValue<std::string>();
            DistFunc::validate(funcName);
            distFunc = getDistFuncType(funcName);
        } else if (Alpha::NAME == lowerCaseName) {
            value.validateType(Alpha::TYPE);
            alpha = value.getValue<double>();
            Alpha::validate(alpha);
        } else if (Efc::NAME == lowerCaseName) {
            value.validateType(Efc::TYPE);
            efc = value.getValue<int64_t>();
            Efc::validate(efc);
        } else {
            throw common::BinderException{
                common::stringFormat("Unrecognized optional parameter {} in {}.", name,
                    function::CreateHNSWIndexFunction::name)};
        }
    }
}

std::string HNSWIndexConfig::distFuncToString(DistFuncType distFunc) {
    switch (distFunc) {
    case DistFuncType::Cosine: {
        return "cosine";
    }
    case DistFuncType::L2: {
        return "l2";
    }
    case DistFuncType::L2_SQUARE: {
        return "l2sq";
    }
    case DistFuncType::DotProduct: {
        return "dotproduct";
    }
    default: {
        throw common::RuntimeException(common::stringFormat("Unknown distance function type {}.",
            static_cast<int64_t>(distFunc)));
    }
    }
}

void HNSWIndexConfig::serialize(common::Serializer& ser) const {
    ser.writeDebuggingInfo("degreeInUpperLayer");
    ser.serializeValue(mu);
    ser.writeDebuggingInfo("degreeInLowerLayer");
    ser.serializeValue(ml);
    ser.writeDebuggingInfo("distFunc");
    ser.serializeValue<uint8_t>(static_cast<uint8_t>(distFunc));
    ser.writeDebuggingInfo("alpha");
    ser.serializeValue(alpha);
    ser.writeDebuggingInfo("efc");
    ser.serializeValue(efc);
}

HNSWIndexConfig HNSWIndexConfig::deserialize(common::Deserializer& deSer) {
    auto config = HNSWIndexConfig{};
    std::string debugginInfo;
    deSer.validateDebuggingInfo(debugginInfo, "degreeInUpperLayer");
    deSer.deserializeValue(config.mu);
    deSer.validateDebuggingInfo(debugginInfo, "degreeInLowerLayer");
    deSer.deserializeValue(config.ml);
    deSer.validateDebuggingInfo(debugginInfo, "distFunc");
    uint8_t distFunc = 0;
    deSer.deserializeValue(distFunc);
    config.distFunc = static_cast<DistFuncType>(distFunc);
    deSer.validateDebuggingInfo(debugginInfo, "alpha");
    deSer.deserializeValue(config.alpha);
    deSer.validateDebuggingInfo(debugginInfo, "efc");
    deSer.deserializeValue(config.efc);
    return config;
}

DistFuncType HNSWIndexConfig::getDistFuncType(const std::string& funcName) {
    const auto lowerFuncName = common::StringUtils::getLower(funcName);
    if (lowerFuncName == "cosine") {
        return DistFuncType::Cosine;
    }
    if (lowerFuncName == "l2") {
        return DistFuncType::L2;
    }
    if (lowerFuncName == "l2sq") {
        return DistFuncType::L2_SQUARE;
    }
    if (lowerFuncName == "dotproduct") {
        return DistFuncType::DotProduct;
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
        } else {
            throw common::BinderException{
                common::stringFormat("Unrecognized optional parameter {} in {}.", name,
                    function::QueryHNSWIndexFunction::name)};
        }
    }
}

} // namespace storage
} // namespace kuzu
