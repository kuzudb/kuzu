#pragma once

#include "main/attached_database.h"

namespace kuzu {
namespace binder {
struct AttachOption;
}

namespace storage {

using attach_function_t = std::unique_ptr<main::AttachedDatabase> (*)(std::string dbPath,
    std::string dbName, main::ClientContext* clientContext,
    const binder::AttachOption& attachOption);

class StorageExtension {
public:
    explicit StorageExtension(attach_function_t attachFunction)
        : attachFunction{std::move(attachFunction)} {}
    virtual bool canHandleDB(std::string /*dbType*/) const { return false; }

    std::unique_ptr<main::AttachedDatabase> attach(std::string dbPath, std::string dbName,
        main::ClientContext* clientContext, const binder::AttachOption& attachOption) const {
        return attachFunction(dbPath, dbName, clientContext, attachOption);
    }

    virtual ~StorageExtension() = default;

private:
    attach_function_t attachFunction;
};

} // namespace storage
} // namespace kuzu
