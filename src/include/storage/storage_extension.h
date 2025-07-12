#pragma once

#include "main/database_instance.h"

namespace kuzu {
namespace binder {
struct AttachOption;
}

namespace storage {

using attach_function_t = std::unique_ptr<main::DatabaseInstance> (*)(std::string dbPath,
    std::string dbName, main::ClientContext* clientContext,
    const binder::AttachOption& attachOption);

class StorageExtension {
public:
    explicit StorageExtension(attach_function_t attachFunction) : attachFunction{attachFunction} {}
    virtual bool canHandleDB(std::string /*dbType*/) const { return false; }

    std::unique_ptr<main::DatabaseInstance> attach(std::string dbName, std::string dbPath,
        main::ClientContext* clientContext, const binder::AttachOption& attachOption) const {
        return attachFunction(std::move(dbName), std::move(dbPath), clientContext, attachOption);
    }

    virtual ~StorageExtension() = default;

private:
    attach_function_t attachFunction;
};

} // namespace storage
} // namespace kuzu
