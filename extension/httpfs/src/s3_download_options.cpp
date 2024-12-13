#include "s3_download_options.h"

#include "extension/extension.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace httpfs {

using namespace common;

void S3DownloadOptions::registerExtensionOptions(main::Database* db) {
    ADD_EXTENSION_OPTION(S3AccessKeyID);
    ADD_EXTENSION_OPTION(S3SecretAccessKey);
    ADD_EXTENSION_OPTION(S3EndPoint);
    ADD_EXTENSION_OPTION(S3URLStyle);
    ADD_EXTENSION_OPTION(S3Region);
}

void S3DownloadOptions::setEnvValue(main::ClientContext* context) {
    auto accessKeyID = main::ClientContext::getEnvVariable(S3AccessKeyID::NAME);
    auto secretAccessKey = main::ClientContext::getEnvVariable(S3SecretAccessKey::NAME);
    auto endpoint = main::ClientContext::getEnvVariable(S3EndPoint::NAME);
    auto urlStyle = main::ClientContext::getEnvVariable(S3URLStyle::NAME);
    auto region = main::ClientContext::getEnvVariable(S3Region::NAME);
    if (accessKeyID != "") {
        context->setExtensionOption(S3AccessKeyID::NAME, Value::createValue(accessKeyID));
    }
    if (secretAccessKey != "") {
        context->setExtensionOption(S3SecretAccessKey::NAME, Value::createValue(secretAccessKey));
    }
    if (endpoint != "") {
        context->setExtensionOption(S3EndPoint::NAME, Value::createValue(endpoint));
    }
    if (urlStyle != "") {
        context->setExtensionOption(S3URLStyle::NAME, Value::createValue(urlStyle));
    }
    if (region != "") {
        context->setExtensionOption(S3Region::NAME, Value::createValue(region));
    }
}

} // namespace httpfs
} // namespace kuzu
