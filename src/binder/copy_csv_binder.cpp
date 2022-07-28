#include "include/copy_csv_binder.h"

namespace graphflow {
namespace binder {

unique_ptr<BoundCopyCSV> CopyCSVBinder::bind(const CopyCSV& copyCSV) {
    auto labelName = copyCSV.getLabelName();
    validateLabelName(labelName);
    auto isNodeLabel = catalog->getReadOnlyVersion()->containNodeLabel(labelName);
    auto labelID = isNodeLabel ? catalog->getReadOnlyVersion()->getNodeLabelFromName(labelName) :
                                 catalog->getReadOnlyVersion()->getRelLabelFromName(labelName);
    return make_unique<BoundCopyCSV>(copyCSV.getCSVFileName(), labelID, isNodeLabel,
        bindParsingOptions(copyCSV.getParsingOptions()));
}

void CopyCSVBinder::validateParsingOptionName(string& parsingOptionName) {
    for (auto i = 0; i < size(LoaderConfig::CSV_PARSING_OPTIONS); i++) {
        if (parsingOptionName == LoaderConfig::CSV_PARSING_OPTIONS[i]) {
            return;
        }
    }
    throw BinderException("Unrecognized parsing csv option: " + parsingOptionName + ".");
}

unordered_map<string, char> CopyCSVBinder::bindParsingOptions(
    unordered_map<string, string> parsingOptions) {
    unordered_map<string, char> boundParsingOptions = getDefaultParsingOptions();
    for (auto& parsingOption : parsingOptions) {
        auto loadOptionName = parsingOption.first;
        validateParsingOptionName(loadOptionName);
        boundParsingOptions[loadOptionName] = bindParsingOptionValue(parsingOption.second);
    }
    return boundParsingOptions;
}

char CopyCSVBinder::bindParsingOptionValue(string parsingOptionValue) {
    if (parsingOptionValue.length() > 2 ||
        (parsingOptionValue.length() == 2 && parsingOptionValue[0] != '\\')) {
        throw BinderException("Copy csv option value can only be a single character with an "
                              "optional escape character.");
    }
    return parsingOptionValue[parsingOptionValue.length() - 1];
}

} // namespace binder
} // namespace graphflow
