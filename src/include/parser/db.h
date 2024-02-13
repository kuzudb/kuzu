#pragma once

#include "common/copier_config/reader_config.h"
#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class ExportDB : public Statement {
public:
    explicit ExportDB(std::string filePath)
        : Statement{common::StatementType::EXPORT_DATABASE}, filePath{std::move(filePath)} {
        fileType = common::FileType::CSV;
    }

    inline void setParsingOption(parsing_option_t options) { parsingOptions = std::move(options); }
    inline void setFileType(std::string type) {
        fileType = common::FileTypeUtils::fromString(type);
    }
    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }
    inline std::string getFilePath() const { return filePath; }
    inline common::FileType getFileType() const { return fileType; }

private:
    parsing_option_t parsingOptions;
    std::string filePath;
    common::FileType fileType;
};

} // namespace parser
} // namespace kuzu
