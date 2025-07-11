#include "catalog/catalog.h"
#include "common/exception/runtime.h"
#include "file_system/azure_filesystem.h"
#include "function/azure_scan.h"

namespace kuzu {
namespace azure_extension {

using namespace kuzu::function;
using namespace kuzu::common;

std::unique_ptr<common::FileInfo> AzureFileSystem::openFile(const std::string& path,
    common::FileOpenFlags /*flags*/, main::ClientContext* /*context*/) {
    return std::make_unique<AzureFileInfo>(path, this);
}

bool AzureFileSystem::canHandleFile(const std::string_view path) const {
    return path.rfind("az://", 0) == 0 || path.rfind("abfss://", 0) == 0;
}

void AzureFileSystem::syncFile(const FileInfo& /*fileInfo*/) const {
    throw RuntimeException("This feature is not currently supported");
}

void AzureFileSystem::readFromFile(FileInfo& /*fileInfo*/, void* /*buffer*/, uint64_t /*numBytes*/,
    uint64_t /*position*/) const {
    throw RuntimeException("This feature is not currently supported");
}

int64_t AzureFileSystem::readFile(FileInfo& /*fileInfo*/, void* /*buf*/,
    size_t /*numBytes*/) const {
    throw RuntimeException("This feature is not currently supported");
}

int64_t AzureFileSystem::seek(FileInfo& /*fileInfo*/, uint64_t /*offset*/, int /*whence*/) const {
    throw RuntimeException("This feature is not currently supported");
}

uint64_t AzureFileSystem::getFileSize(const FileInfo& /*fileInfo*/) const {
    throw RuntimeException("This feature is not currently supported");
}

bool AzureFileSystem::fileOrPathExists(const std::string& /*path*/,
    main::ClientContext* /*context*/) {
    return true;
}

TableFunction AzureFileSystem::getHandleFunction(const std::string& /*path*/) const {
    return *AzureScanFunction::getFunctionSet()[0]->constPtrCast<TableFunction>();
}

} // namespace azure_extension
} // namespace kuzu
