#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/copy_constructors.h"
#include "common/enums/scan_source_type.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

struct BaseScanSource {
    common::ScanSourceType type;

    explicit BaseScanSource(common::ScanSourceType type) : type{type} {}
    virtual ~BaseScanSource() = default;
    DELETE_COPY_AND_MOVE(BaseScanSource);
};

struct FileScanSource : public BaseScanSource {
    std::vector<std::string> filePaths;

    explicit FileScanSource(std::vector<std::string> paths)
        : BaseScanSource{common::ScanSourceType::FILE}, filePaths{std::move(paths)} {}
};

struct ObjectScanSource : public BaseScanSource {
    std::string objectName;

    explicit ObjectScanSource(std::string objectName)
        : BaseScanSource{common::ScanSourceType::OBJECT}, objectName{std::move(objectName)} {}
};

struct QueryScanSource : public BaseScanSource {
    std::unique_ptr<Statement> statement;

    explicit QueryScanSource(std::unique_ptr<Statement> statement)
        : BaseScanSource{common::ScanSourceType::QUERY}, statement{std::move(statement)} {}
};

} // namespace parser
} // namespace kuzu
