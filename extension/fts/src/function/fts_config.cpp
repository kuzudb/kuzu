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
#include "re2.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

void Stemmer::validate(const std::string& stemmer) {
    StemFunction::validateStemmer(stemmer);
}

StopWordsTableInfo::StopWordsTableInfo()
    : stopWords{StopWords::DEFAULT_VALUE}, tableName{FTSUtils::getDefaultStopWordsTableName()},
      source{StopWordsSource::DEFAULT} {}

const std::unordered_set<std::string>& StopWords::getDefaultStopWords() {
    static const std::unordered_set<std::string> defaultStopWords = {"a", "a's", "able", "about",
        "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again",
        "against", "ain't", "all", "allow", "allows", "almost", "alone", "along", "already", "also",
        "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody",
        "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear",
        "appreciate", "appropriate", "are", "aren't", "around", "as", "aside", "ask", "asking",
        "associated", "at", "available", "away", "awfully", "b", "be", "became", "because",
        "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
        "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both",
        "brief", "but", "by", "c", "c'mon", "c's", "came", "can", "can't", "cannot", "cant",
        "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come",
        "comes", "concerning", "consequently", "consider", "considering", "contain", "containing",
        "contains", "corresponding", "could", "couldn't", "course", "currently", "d", "definitely",
        "described", "despite", "did", "didn't", "different", "do", "does", "doesn't", "doing",
        "don't", "done", "down", "downwards", "during", "e", "each", "edu", "eg", "eight", "either",
        "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever",
        "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example",
        "except", "f", "far", "few", "fifth", "first", "five", "followed", "following", "follows",
        "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "g", "get",
        "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten",
        "greetings", "h", "had", "hadn't", "happens", "hardly", "has", "hasn't", "have", "haven't",
        "having", "he", "he's", "hello", "help", "hence", "her", "here", "here's", "hereafter",
        "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither",
        "hopefully", "how", "howbeit", "however", "i", "i'd", "i'll", "i'm", "i've", "ie", "if",
        "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated",
        "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isn't", "it", "it'd",
        "it'll", "it's", "its", "itself", "j", "just", "k", "keep", "keeps", "kept", "know",
        "knows", "known", "l", "last", "lately", "later", "latter", "latterly", "least", "less",
        "lest", "let", "let's", "like", "liked", "likely", "little", "look", "looking", "looks",
        "ltd", "m", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might",
        "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "n", "name", "namely",
        "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless",
        "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not",
        "nothing", "novel", "now", "nowhere", "o", "obviously", "of", "off", "often", "oh", "ok",
        "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others",
        "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall",
        "own", "p", "particular", "particularly", "per", "perhaps", "placed", "please", "plus",
        "possible", "presumably", "probably", "provides", "q", "que", "quite", "qv", "r", "rather",
        "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively",
        "respectively", "right", "s", "said", "same", "saw", "say", "saying", "says", "second",
        "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves",
        "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should",
        "shouldn't", "since", "six", "so", "some", "somebody", "somehow", "someone", "something",
        "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify",
        "specifying", "still", "sub", "such", "sup", "sure", "t", "t's", "take", "taken", "tell",
        "tends", "th", "than", "thank", "thanks", "thanx", "that", "that's", "thats", "the",
        "their", "theirs", "them", "themselves", "then", "thence", "there", "there's", "thereafter",
        "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "they'd",
        "they'll", "they're", "they've", "think", "third", "this", "thorough", "thoroughly",
        "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together",
        "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice",
        "two", "u", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up",
        "upon", "us", "use", "used", "useful", "uses", "using", "usually", "uucp", "v", "value",
        "various", "very", "via", "viz", "vs", "w", "want", "wants", "was", "wasn't", "way", "we",
        "we'd", "we'll", "we're", "we've", "welcome", "well", "went", "were", "weren't", "what",
        "what's", "whatever", "when", "whence", "whenever", "where", "where's", "whereafter",
        "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
        "whither", "who", "who's", "whoever", "whole", "whom", "whose", "why", "will", "willing",
        "wish", "with", "within", "without", "won't", "wonder", "would", "wouldn't", "x", "y",
        "yes", "yet", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
        "yourselves", "z", "zero"};
    return defaultStopWords;
};

void IgnorePattern::validate(const std::string& ignorePattern) {
    const RE2 regexPattern(ignorePattern);
    if (!regexPattern.ok()) {
        throw common::BinderException{
            common::stringFormat("An error occurred while compiling the regex: \"{}\"."
                                 "\nError: \"{}\".",
                ignorePattern, regexPattern.error())};
    }
}

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
        } else if (IgnorePattern::NAME == lowerCaseName) {
            value.validateType(IgnorePattern::TYPE);
            ignorePattern = common::StringUtils::getLower(value.getValue<std::string>());
            ignorePatternQuery = ignorePattern;
            common::StringUtils::replaceAll(ignorePatternQuery, "*", "");
            common::StringUtils::replaceAll(ignorePatternQuery, "?", "");
            IgnorePattern::validate(ignorePattern);
            IgnorePattern::validate(ignorePatternQuery);
        } else {
            throw common::BinderException{"Unrecognized optional parameter: " + name};
        }
    }
}

FTSConfig CreateFTSConfig::getFTSConfig() const {
    return FTSConfig{stemmer, stopWordsTableInfo.tableName, stopWordsTableInfo.stopWords,
        ignorePattern, ignorePatternQuery};
}

void FTSConfig::serialize(common::Serializer& serializer) const {
    serializer.serializeValue(stemmer);
    serializer.serializeValue(stopWordsTableName);
    serializer.serializeValue(stopWordsSource);
    serializer.serializeValue(ignorePattern);
    serializer.serializeValue(ignorePatternQuery);
}

FTSConfig FTSConfig::deserialize(common::Deserializer& deserializer) {
    auto config = FTSConfig{};
    deserializer.deserializeValue(config.stemmer);
    deserializer.deserializeValue(config.stopWordsTableName);
    deserializer.deserializeValue(config.stopWordsSource);
    deserializer.deserializeValue(config.ignorePattern);
    deserializer.deserializeValue(config.ignorePatternQuery);
    return config;
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

void TopK::validate(uint64_t value) {
    if (value == 0) {
        throw common::BinderException{
            "QUERY_FTS_INDEX requires a positive non-zero value for the 'top' parameter."};
    }
}

} // namespace fts_extension
} // namespace kuzu
