#include "binder/bind/bound_copy_csv.h"
#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "parser/copy_csv/copy_csv.h"

namespace kuzu {
namespace binder {

unique_ptr<BoundStatement> Binder::bindCopyCSV(const Statement& statement) {
    auto& copyCSV = (CopyCSV&)statement;
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableName = copyCSV.getTableName();
    validateTableExist(catalog, tableName);
    auto isNodeTable = catalogContent->containNodeTable(tableName);
    auto tableID = isNodeTable ? catalogContent->getNodeTableIDFromName(tableName) :
                                 catalogContent->getRelTableIDFromName(tableName);
    auto filePath = copyCSV.getCSVFileName();
    auto csvReaderConfig = bindParsingOptions(copyCSV.getParsingOptions());
    return make_unique<BoundCopyCSV>(
        CSVDescription(filePath, csvReaderConfig), TableSchema(tableName, tableID, isNodeTable));
}

CSVReaderConfig Binder::bindParsingOptions(
    const unordered_map<string, unique_ptr<ParsedExpression>>* parsingOptions) {
    CSVReaderConfig csvReaderConfig;
    for (auto& parsingOption : *parsingOptions) {
        auto copyOptionName = parsingOption.first;
        bool isValidStringParsingOption = validateStringParsingOptionName(copyOptionName);
        auto copyOptionExpression = parsingOption.second.get();
        auto boundCopyOptionExpression = expressionBinder.bindExpression(*copyOptionExpression);
        assert(boundCopyOptionExpression->expressionType = LITERAL);
        if (copyOptionName == "HEADER") {
            if (boundCopyOptionExpression->dataType.typeID != BOOL) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be boolean.");
            }
            csvReaderConfig.hasHeader =
                ((LiteralExpression&)(*boundCopyOptionExpression)).literal->val.booleanVal;
        } else if (boundCopyOptionExpression->dataType.typeID == STRING &&
                   isValidStringParsingOption) {
            if (boundCopyOptionExpression->dataType.typeID != STRING) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be string.");
            }
            auto copyOptionValue =
                ((LiteralExpression&)(*boundCopyOptionExpression)).literal->strVal;
            bindStringParsingOptions(csvReaderConfig, copyOptionName, copyOptionValue);
        } else {
            throw BinderException("Unrecognized parsing csv option: " + copyOptionName + ".");
        }
    }
    return csvReaderConfig;
}

void Binder::bindStringParsingOptions(
    CSVReaderConfig& csvReaderConfig, const string& optionName, string& optionValue) {
    auto parsingOptionValue = bindParsingOptionValue(optionValue);
    if (optionName == "ESCAPE") {
        csvReaderConfig.escapeChar = parsingOptionValue;
    } else if (optionName == "DELIM") {
        csvReaderConfig.tokenSeparator = parsingOptionValue;
    } else if (optionName == "QUOTE") {
        csvReaderConfig.quoteChar = parsingOptionValue;
    } else if (optionName == "LIST_BEGIN") {
        csvReaderConfig.listBeginChar = parsingOptionValue;
    } else if (optionName == "LIST_END") {
        csvReaderConfig.listEndChar = parsingOptionValue;
    }
}

char Binder::bindParsingOptionValue(string value) {
    if (value.length() > 2 || (value.length() == 2 && value[0] != '\\')) {
        throw BinderException("Copy csv option value can only be a single character with an "
                              "optional escape character.");
    }
    return value[value.length() - 1];
}

} // namespace binder
} // namespace kuzu
