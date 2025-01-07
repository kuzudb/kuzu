#include "function/fts_config.h"

#include "common/exception/binder.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"
#include "function/stem.h"

namespace kuzu {
namespace fts_extension {

void Stemmer::validate(const std::string& stemmer) {
    StemFunction::validateStemmer(stemmer);
}

FTSConfig::FTSConfig(const function::optional_params_t& optionalParams) {
    for (auto& [name, value] : optionalParams) {
        auto lowerCaseName = common::StringUtils::getLower(name);
        if (Stemmer::NAME == lowerCaseName) {
            value.validateType(Stemmer::TYPE);
            stemmer = common::StringUtils::getLower(value.getValue<std::string>());
            Stemmer::validate(stemmer);
        } else {
            throw common::BinderException{"Unrecognized optional parameter: " + name};
        }
    }
}

void FTSConfig::serialize(common::Serializer& serializer) const {
    serializer.serializeValue(stemmer);
}

FTSConfig FTSConfig::deserialize(common::Deserializer& deserializer) {
    auto config = FTSConfig{};
    deserializer.deserializeValue(config.stemmer);
    return config;
}

// We store the length + the string itself. So the total size is sizeof(length) + string length.
uint64_t FTSConfig::getNumBytesForSerialization() const {
    return sizeof(stemmer.size()) + stemmer.size();
}

void K::validate(double value) {
    if (value < 0) {
        throw common::BinderException{
            "BM25 model requires the Term Frequency Saturation(k) value to be a positive number."};
    }
}

void B::validate(double value) {
    if (value < 0 || value > 1) {
        throw common::BinderException{
            "BM25 model requires the Document Length Normalization(b) value to be in the range "
            "[0,1]."};
    }
}

QueryFTSConfig::QueryFTSConfig(const function::optional_params_t& optionalParams) {
    for (auto& [name, value] : optionalParams) {
        auto lowerCaseName = common::StringUtils::getLower(name);
        if (K::NAME == lowerCaseName) {
            value.validateType(K::TYPE);
            k = value.getValue<double>();
            K::validate(k);
        } else if (B::NAME == lowerCaseName) {
            value.validateType(B::TYPE);
            b = value.getValue<double>();
            B::validate(b);
        } else if (Conjunctive::NAME == lowerCaseName) {
            value.validateType(Conjunctive::TYPE);
            isConjunctive = value.getValue<bool>();
        } else {
            throw common::BinderException{"Unrecognized optional parameter: " + name};
        }
    }
}

} // namespace fts_extension
} // namespace kuzu
