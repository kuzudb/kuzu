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

enum class StopWordsSource : uint8_t {
    FILE = 0,
    TABLE = 1,
    DEFAULT = 2,
};

struct StopWordsTableInfo {
    std::string stopWords;
    std::string tableName;
    StopWordsSource source;

    StopWordsTableInfo();
    StopWordsTableInfo(std::string stopWords, std::string tableName, StopWordsSource source)
        : stopWords{std::move(stopWords)}, tableName{std::move(tableName)}, source{source} {}
};

struct StopWords {
    static constexpr const char* NAME = "stopwords";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static constexpr const char* DEFAULT_VALUE = "default";
    static const std::unordered_set<std::string>& getDefaultStopWords();
    static constexpr uint64_t NUM_DEFAULT_STOP_WORDS = 570;

    static StopWordsTableInfo bind(main::ClientContext& context, common::table_id_t tableID,
        const std::string& indexName, const std::string& stopWords);
};

struct IgnorePattern {
    static constexpr const char* NAME = "ignore_pattern";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::STRING;
    static constexpr const char* DEFAULT_VALUE = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    // We store a second ignore pattern for queries to not replace on wildcard characters
    static constexpr const char* DEFAULT_VALUE_QUERY =
        "[0-9!@#$%^&()_+={}\\[\\]:;<>,.~\\/\\|'\"`-]+";

    static void validate(const std::string& ignorePattern);
};

struct FTSConfig;

struct CreateFTSConfig {
    std::string stemmer = Stemmer::DEFAULT_VALUE;
    StopWordsTableInfo stopWordsTableInfo;
    std::string ignorePattern = IgnorePattern::DEFAULT_VALUE;
    std::string ignorePatternQuery = IgnorePattern::DEFAULT_VALUE_QUERY;

    CreateFTSConfig() = default;
    CreateFTSConfig(main::ClientContext& context, common::table_id_t tableID,
        const std::string& indexName, const function::optional_params_t& optionalParams);

    FTSConfig getFTSConfig() const;
};

struct FTSConfig {
    std::string stemmer = "";
    std::string stopWordsTableName = "";
    // The original stopwords that the user used when creating the index. This field is only
    // used by show_index.
    std::string stopWordsSource = "";
    std::string ignorePattern = "";
    std::string ignorePatternQuery = "";

    FTSConfig() = default;
    FTSConfig(std::string stemmer, std::string stopWordsTableName, std::string stopWordsSource,
        std::string ignorePattern, std::string ignorePatternQuery)
        : stemmer{std::move(stemmer)}, stopWordsTableName{std::move(stopWordsTableName)},
          stopWordsSource{std::move(stopWordsSource)}, ignorePattern{std::move(ignorePattern)},
          ignorePatternQuery{std::move(ignorePatternQuery)} {}

    void serialize(common::Serializer& serializer) const;

    static FTSConfig deserialize(common::Deserializer& deserializer);
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

struct TopK {
    static constexpr const char* NAME = "top";
    static constexpr common::LogicalTypeID TYPE = common::LogicalTypeID::UINT64;
    static constexpr uint64_t DEFAULT_VALUE = UINT64_MAX;
    static void validate(uint64_t value);
};

} // namespace fts_extension
} // namespace kuzu
