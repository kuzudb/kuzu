#include "binder/binder.h"
#include "binder/bound_extension_statement.h"
#include "common/exception/binder.h"
#include "common/file_system/local_file_system.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "parser/extension_statement.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

static void bindInstallExtension(const ExtensionAuxInfo& auxInfo) {
    if (!ExtensionUtils::isOfficialExtension(auxInfo.path)) {
        throw common::BinderException(
            common::stringFormat("{} is not an official extension.\nNon-official extensions "
                                 "can be installed directly by: `LOAD EXTENSION [EXTENSION_PATH]`.",
                auxInfo.path));
    }
}

static void bindLoadExtension(const ExtensionAuxInfo& auxInfo) {
    if (ExtensionUtils::isOfficialExtension(auxInfo.path)) {
        return;
    }
    auto localFileSystem = common::LocalFileSystem("");
    if (!localFileSystem.fileOrPathExists(auxInfo.path, nullptr /* clientContext */)) {
        throw common::BinderException(
            common::stringFormat("The extension {} is neither an official extension, nor does "
                                 "the extension path: '{}' exists.",
                auxInfo.path, auxInfo.path));
    }
}

std::unique_ptr<BoundStatement> Binder::bindExtension(const Statement& statement) {
    auto extensionStatement = statement.constPtrCast<ExtensionStatement>();
    auto auxInfo = extensionStatement->getAuxInfo();
    switch (auxInfo->action) {
    case ExtensionAction::INSTALL:
        bindInstallExtension(*auxInfo);
        break;
    case ExtensionAction::LOAD:
        bindLoadExtension(*auxInfo);
        break;
    default:
        KU_UNREACHABLE;
    }
    if (ExtensionUtils::isOfficialExtension(auxInfo->path)) {
        common::StringUtils::toLower(auxInfo->path);
    }
    return std::make_unique<BoundExtensionStatement>(std::move(auxInfo));
}

} // namespace binder
} // namespace kuzu
