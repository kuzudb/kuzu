#pragma once

#include "src/binder/bound_copy_csv/include/bound_copy_csv.h"
#include "src/catalog/include/catalog.h"
#include "src/common/include/configs.h"
#include "src/parser/copy_csv/include/copy_csv.h"

using namespace graphflow::parser;
using namespace graphflow::catalog;

namespace graphflow {
namespace binder {

class CopyCSVBinder {
public:
    explicit CopyCSVBinder(Catalog* catalog) : catalog{catalog} {}

    unique_ptr<BoundCopyCSV> bind(const CopyCSV& copyCSV);

    /******* validations *********/
    inline void validateLabelName(string& schemaName) {
        if (!catalog->getReadOnlyVersion()->containNodeLabel(schemaName) &&
            !catalog->getReadOnlyVersion()->containRelLabel(schemaName)) {
            throw BinderException("Node/Rel " + schemaName + " does not exist.");
        }
    }

    static void validateParsingOptionName(string& parsingOptionName);

    static unordered_map<string, char> bindParsingOptions(
        unordered_map<string, string> parsingOptions);

    static char bindParsingOptionValue(string parsingOptionValue);

    static unordered_map<string, char> getDefaultParsingOptions() {
        return {{"ESCAPE", LoaderConfig::DEFAULT_ESCAPE_CHAR},
            {"DELIM", LoaderConfig::DEFAULT_TOKEN_SEPARATOR},
            {"QUOTE", LoaderConfig::DEFAULT_QUOTE_CHAR},
            {"LIST_BEGIN", LoaderConfig::DEFAULT_LIST_BEGIN_CHAR},
            {"LIST_END", LoaderConfig::DEFAULT_LIST_END_CHAR}};
    }

private:
    Catalog* catalog;
};

} // namespace binder
} // namespace graphflow
