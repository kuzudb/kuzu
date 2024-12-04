#include "binder/binder.h"
#include "binder/bound_extension_statement.h"
#include "common/exception/binder.h"
#include "common/file_system/local_file_system.h"
#include "extension/extension.h"
#include "parser/extension_statement.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

static void bindInstallExtension(const ExtensionStatement* extensionStatement) {
    if (extensionStatement->getAction() == ExtensionAction::INSTALL) {
        auto extensionName = extensionStatement->getPath();
        if (!ExtensionUtils::isOfficialExtension(extensionName)) {
            throw common::BinderException(common::stringFormat(
                "{} is not an official extension.\nNon-official extensions "
                "can be installed directly by: `LOAD EXTENSION [EXTENSION_PATH]`.",
                extensionName));
        }
    }
}

static void bindLoadExtension(const ExtensionStatement* extensionStatement) {
    if (ExtensionUtils::isOfficialExtension(extensionStatement->getPath())) {
        return;
    }
    auto localFileSystem = common::LocalFileSystem("");
    if (!localFileSystem.fileOrPathExists(extensionStatement->getPath(),
            nullptr /* clientContext */)) {
        throw common::BinderException(
            common::stringFormat("The extension {} is neither an official extension, nor does "
                                 "the extension path: '{}' exists.",
                extensionStatement->getPath(), extensionStatement->getPath()));
    }
}

std::unique_ptr<BoundStatement> Binder::bindExtension(const Statement& statement) {
    auto extensionStatement = statement.constPtrCast<ExtensionStatement>();
    switch (extensionStatement->getAction()) {
    case ExtensionAction::INSTALL:
        bindInstallExtension(extensionStatement);
        break;
    case ExtensionAction::LOAD:
        bindLoadExtension(extensionStatement);
        break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<BoundExtensionStatement>(extensionStatement->getAction(),
        extensionStatement->getPath());
}

} // namespace binder
} // namespace kuzu
