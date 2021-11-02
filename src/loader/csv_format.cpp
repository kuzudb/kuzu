#include "src/loader/include/csv_format.h"

#include "src/common/include/exception.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

void CSVFormat::parseCsvFormat(unique_ptr<nlohmann::json>& parsedJson) {
    char tokenSeparator = ',';
    char quoteChar = '"';
    char escapeChar = '\\';
    getCSVConfig(*parsedJson, "tokenSeparator", tokenSeparator);
    getCSVConfig(*parsedJson, "quoteCharacter", quoteChar);
    getCSVConfig(*parsedJson, "escapeCharacter", escapeChar);
    auto nodeSize = parsedJson->at("nodeFileDescriptions").size();
    auto relSize = parsedJson->at("relFileDescriptions").size();
    nodeFileTokenSeparators = vector<char>(nodeSize, tokenSeparator);
    relFileTokenSeparators = vector<char>(relSize, tokenSeparator);
    nodeFileQuoteChars = vector<char>(nodeSize, quoteChar);
    relFileQuoteChars = vector<char>(relSize, quoteChar);
    nodeFileEscapeChars = vector<char>(nodeSize, escapeChar);
    relFileEscapeChars = vector<char>(relSize, escapeChar);
}

void CSVFormat::updateNodeFormat(nlohmann::json& parsedNodeFileDescription, uint32_t& nodeIndex) {
    getCSVConfig(parsedNodeFileDescription, "tokenSeparator", nodeFileTokenSeparators[nodeIndex]);
    getCSVConfig(parsedNodeFileDescription, "quoteCharacter", nodeFileQuoteChars[nodeIndex]);
    getCSVConfig(parsedNodeFileDescription, "escapeCharacter", nodeFileEscapeChars[nodeIndex]);
}

void CSVFormat::updateRelFormat(nlohmann::json& parsedRelFileDescription, uint32_t& relIndex) {
    getCSVConfig(parsedRelFileDescription, "tokenSeparator", relFileTokenSeparators[relIndex]);
    getCSVConfig(parsedRelFileDescription, "quoteCharacter", relFileQuoteChars[relIndex]);
    getCSVConfig(parsedRelFileDescription, "escapeCharacter", relFileEscapeChars[relIndex]);
}

void CSVFormat::getCSVConfig(nlohmann::json& parsedFileDescription, string key, char& val) {
    if (parsedFileDescription.contains(key)) {
        if (parsedFileDescription.at(key).get<string>().length() != 1) {
            throw LoaderException(key + " must be a single character!");
        }
        val = parsedFileDescription.at(key).get<string>()[0];
    }
}

} // namespace loader
} // namespace graphflow
