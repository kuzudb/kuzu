#pragma once

#include "common/file_system/file_system.h"
#include "function/table/table_function.h"
#include "main/client_context.h"

namespace kuzu {
namespace azure_extension {

struct AzureFileInfo final : public common::FileInfo {
    AzureFileInfo(const std::string& path, common::FileSystem* fs) : common::FileInfo{path, fs} {}
};

class AzureFileSystem final : public common::FileSystem {
public:
    std::unique_ptr<common::FileInfo> openFile(const std::string& path, common::FileOpenFlags flags,
        main::ClientContext* context = nullptr) override;

    bool canHandleFile(const std::string_view path) const override;

private:
    void syncFile(const common::FileInfo& fileInfo) const override;

    void readFromFile(common::FileInfo& fileInfo, void* buffer, uint64_t numBytes,
        uint64_t position) const override;

    int64_t readFile(common::FileInfo& fileInfo, void* buf, size_t numBytes) const override;

    int64_t seek(common::FileInfo& fileInfo, uint64_t offset, int whence) const override;

    uint64_t getFileSize(const common::FileInfo& fileInfo) const override;

    bool fileOrPathExists(const std::string& path, main::ClientContext* context = nullptr) override;

    bool handleFileViaFunction(const std::string& /*path*/) const override { return true; }

    function::TableFunction getHandleFunction(const std::string& /*path*/) const override;
};

} // namespace azure_extension
} // namespace kuzu
