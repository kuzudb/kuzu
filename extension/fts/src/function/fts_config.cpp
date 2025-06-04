#include "function/fts_config.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"
#include "function/stem.h"
#include "main/client_context.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

void Stemmer::validate(const std::string& stemmer) {
    StemFunction::validateStemmer(stemmer);
}

StopWordsTableInfo::StopWordsTableInfo()
    : stopWords{StopWords::DEFAULT_VALUE}, tableName{FTSUtils::getDefaultStopWordsTableName()},
      source{StopWordsSource::DEFAULT} {}

StopWordsTableInfo StopWords::bind(main::ClientContext& context, common::table_id_t tableID,
    const std::string& indexName, const std::string& stopWords) {
    auto catalog = context.getCatalog();
    if (stopWords == DEFAULT_VALUE) {
        return StopWordsTableInfo{stopWords, FTSUtils::getDefaultStopWordsTableName(),
            StopWordsSource::DEFAULT};
    } else if (catalog->containsTable(context.getTransaction(), stopWords)) {
        auto entry =
            context.getCatalog()->getTableCatalogEntry(context.getTransaction(), stopWords);
        if (entry->getTableType() != common::TableType::NODE) {
            throw common::BinderException{"The stop words table must be a node table."};
        }
        auto& nodeTableEntry = entry->constCast<catalog::NodeTableCatalogEntry>();
        if (nodeTableEntry.getNumProperties() != 1 ||
            nodeTableEntry.getPrimaryKeyDefinition().getType() != common::LogicalType::STRING()) {
            throw common::BinderException{
                "The stop words table must have exactly one string column."};
        }
        return StopWordsTableInfo{stopWords,
            FTSUtils::getNonDefaultStopWordsTableName(tableID, indexName), StopWordsSource::TABLE};
    } else {
        if (!context.getVFSUnsafe()->fileOrPathExists(stopWords, &context)) {
            throw common::BinderException{common::stringFormat(
                "Given stopwords: '{}' is not a node table name nor a valid file path.",
                stopWords)};
        }
        return StopWordsTableInfo{stopWords,
            FTSUtils::getNonDefaultStopWordsTableName(tableID, indexName), StopWordsSource::FILE};
    }
}

CreateFTSConfig::CreateFTSConfig(main::ClientContext& context, common::table_id_t tableID,
    const std::string& indexName, const function::optional_params_t& optionalParams) {
    for (auto& [name, value] : optionalParams) {
        auto lowerCaseName = common::StringUtils::getLower(name);
        if (Stemmer::NAME == lowerCaseName) {
            value.validateType(Stemmer::TYPE);
            stemmer = common::StringUtils::getLower(value.getValue<std::string>());
            Stemmer::validate(stemmer);
        } else if (StopWords::NAME == lowerCaseName) {
            value.validateType(StopWords::TYPE);
            stopWordsTableInfo =
                StopWords::bind(context, tableID, indexName, value.getValue<std::string>());
        } else {
            throw common::BinderException{"Unrecognized optional parameter: " + name};
        }
    }
}

void FTSConfig::serialize(common::Serializer& serializer) const {
    serializer.serializeValue(stemmer);
    serializer.serializeValue(stopWordsTableName);
    serializer.serializeValue(stopWordsSource);
}

FTSConfig FTSConfig::deserialize(common::Deserializer& deserializer) {
    auto config = FTSConfig{};
    deserializer.deserializeValue(config.stemmer);
    deserializer.deserializeValue(config.stopWordsTableName);
    deserializer.deserializeValue(config.stopWordsSource);
    return config;
}

// We store the length + the string itself. So the total size is sizeof(length) + string length.
uint64_t FTSConfig::getNumBytesForSerialization() const {
    return sizeof(stemmer.size()) + stemmer.size() + sizeof(stopWordsTableName.size()) +
           stopWordsTableName.size() + sizeof(stopWordsSource.size()) + stopWordsSource.size();
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
