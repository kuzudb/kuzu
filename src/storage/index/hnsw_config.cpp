#include "storage/index/hnsw_config.h"

#include "common/exception/binder.h"
#include "common/string_utils.h"
#include <common/serializer/deserializer.h>
#include <common/serializer/serializer.h>

namespace kuzu {
namespace storage {

void Mu::validate(int64_t value) {
    if (value < 1 || value > 100) {
        throw common::BinderException{"Mu must be a positive integer between 1 and 100."};
    }
}

void Ml::validate(int64_t value) {
    if (value < 1 || value > 200) {
        throw common::BinderException{"Mu must be a positive integer between 1 and 200."};
    }
}

void Pl::validate(double value) {
    if (value < 0 || value > 1) {
        throw common::BinderException{"Pl must be a double between 0 and 1."};
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
    if (value >= std::numeric_limits<uint32_t>::max()) {
        throw common::BinderException{"Efs must be a positive integer."};
    }
}

HNSWIndexConfig::HNSWIndexConfig(const function::optional_params_t& optionalParams) {
    for (auto& [name, value] : optionalParams) {
        auto lowerCaseName = common::StringUtils::getLower(name);
        if (Mu::NAME == lowerCaseName) {
            value.validateType(Mu::TYPE);
            degreeInUpperLayer = value.getValue<int64_t>();
            Mu::validate(degreeInUpperLayer);
        } else if (Ml::NAME == lowerCaseName) {
            value.validateType(Ml::TYPE);
            degreeInLowerLayer = value.getValue<int64_t>();
            Ml::validate(degreeInLowerLayer);
        } else if (Pl::NAME == lowerCaseName) {
            value.validateType(Pl::TYPE);
            upperLayerNodePct = value.getValue<double>();
            Pl::validate(upperLayerNodePct);
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
            efc = value.getValue<uint64_t>();
            Efc::validate(efc);
        } else {
            throw common::BinderException{"Unrecognized optional parameter: " + name};
        }
    }
}

void HNSWIndexConfig::serialize(common::Serializer& ser) const {
    ser.writeDebuggingInfo("degreeInUpperLayer");
    ser.serializeValue(degreeInUpperLayer);
    ser.writeDebuggingInfo("degreeInLowerLayer");
    ser.serializeValue(degreeInLowerLayer);
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
    deSer.deserializeValue(config.degreeInUpperLayer);
    deSer.validateDebuggingInfo(debugginInfo, "degreeInLowerLayer");
    deSer.deserializeValue(config.degreeInLowerLayer);
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
            efs = value.getValue<uint64_t>();
            Efs::validate(efs);
        } else {
            throw common::BinderException{"Unrecognized optional parameter: " + name};
        }
    }
}

} // namespace storage
} // namespace kuzu
