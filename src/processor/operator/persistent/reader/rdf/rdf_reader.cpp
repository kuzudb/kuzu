#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/exception/runtime.h"
#include "common/vector/value_vector.h"
#include "processor/operator/persistent/reader/rdf/rdf_utils.h"
#include "serd.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

RdfReader::~RdfReader() {
    serd_reader_end_stream(reader);
    serd_reader_free(reader);
    serd_env_free(env);
    // Even if the close fails, the stream is in an undefined state. There really isn't anything we
    // can do.
    (void)fclose(fp);
}

static SerdSyntax getSerdSyntax(FileType fileType) {
    switch (fileType) {
    case FileType::TURTLE:
        return SerdSyntax ::SERD_TURTLE;
    case FileType::NQUADS:
        return SerdSyntax ::SERD_NQUADS;
    case FileType::NTRIPLES:
        return SerdSyntax ::SERD_NTRIPLES;
    default:
        KU_UNREACHABLE;
    }
}

void RdfReader::initInternal(SerdStatementSink statementSink) {
    KU_ASSERT(reader == nullptr);
    fp = fopen(this->filePath.c_str(), "rb");
    reader = serd_reader_new(getSerdSyntax(fileType), this, nullptr, baseHandle, prefixHandle,
        statementSink, nullptr);
    serd_reader_set_error_sink(reader, errorHandle, this);
    auto fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
    env = serd_env_new(nullptr);
}

SerdStatus RdfReader::errorHandle(void* handle, const SerdError* error) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (reader->rdfConfig.strict) {
        throw RuntimeException(common::stringFormat("{} while reading file at line {} and col {}",
            (char*)serd_strerror(error->status), error->line, error->col));
    }
    return error->status;
}

SerdStatus RdfReader::baseHandle(void* handle, const SerdNode* baseNode) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    serd_env_set_base_uri(reader->env, baseNode);
    reader->hasBaseUri = true;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::prefixHandle(void* handle, const SerdNode* nameNode,
    const SerdNode* uriNode) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    serd_env_set_prefix(reader->env, nameNode, uriNode);
    return SERD_SUCCESS;
}

offset_t RdfReader::readChunk(DataChunk* dataChunk) {
    if (cursor == store_->size()) {
        cursor = 0;
        store_->clear();
        while (true) {
            if (status == SERD_FAILURE || store_->size() > DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            if (status > SERD_FAILURE) {
                serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            }
            status = serd_reader_read_chunk(reader);
        }
    }
    if (store_->isEmpty()) {
        return 0;
    }
    auto numTuplesRead = readToVector(dataChunk);
    cursor += numTuplesRead;
    dataChunk->state->getSelVectorUnsafe().setSelSize(numTuplesRead);
    return numTuplesRead;
}

void RdfReader::readAll() {
    while (true) {
        status = serd_reader_read_chunk(reader);
        if (status == SERD_FAILURE) {
            return;
        }
        if (status > SERD_FAILURE) {
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
        }
    }
}

static void appendSerdChunk(std::string& str, const SerdChunk& chunk) {
    if (chunk.len != 0) {
        str += std::string((const char*)chunk.buf, chunk.len);
    }
}

std::string RdfReader::getAsString(const SerdNode* node) {
    switch (node->type) {
    case SERD_URI: {
        if (!hasBaseUri || serd_uri_string_has_scheme(node->buf)) {
            return std::string((const char*)node->buf, node->n_bytes);
        }
        SerdURI inputUri;
        serd_uri_parse(node->buf, &inputUri);
        SerdURI baseUri;
        serd_env_get_base_uri(env, &baseUri);
        SerdURI resultUri;
        serd_uri_resolve(&inputUri, &baseUri, &resultUri);
        std::string result;
        appendSerdChunk(result, resultUri.scheme);
        if (resultUri.scheme.len != 0) {
            result += "://";
        }
        appendSerdChunk(result, resultUri.authority);
        appendSerdChunk(result, resultUri.path_base);
        appendSerdChunk(result, resultUri.path);
        appendSerdChunk(result, resultUri.query);
        appendSerdChunk(result, resultUri.fragment);
        return result;
    }
    case SERD_CURIE: {
        SerdChunk prefix = SerdChunk();
        SerdChunk suffix = SerdChunk();
        auto st = serd_env_expand(env, node, &prefix, &suffix);
        if (st == SERD_SUCCESS) {
            return std::string((const char*)prefix.buf, prefix.len) +
                   std::string((const char*)suffix.buf, suffix.len);
        } else if (rdfConfig.strict) {
            throw RuntimeException(stringFormat("Cannot expand {}.",
                std::string((const char*)node->buf, node->n_bytes)));
        }
        return std::string(); // expand fail return empty string.
    }
    case SERD_BLANK: {
        auto label = std::string((const char*)node->buf, node->n_bytes);
        return "_:" + std::to_string(fileIdx) + label;
    }
    default:
        return std::string((const char*)node->buf, node->n_bytes);
    }
}

SerdStatus RdfResourceReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = reader->store_->cast<ResourceStore>();
    auto subjectStr = reader->getAsString(subject);
    auto predicateStr = reader->getAsString(predicate);
    if (object->type == SERD_LITERAL) {
        if (subjectStr.empty() || predicateStr.empty()) { // skip empty iris.
            return SERD_SUCCESS;
        }
        // Add subject and predicate as resource.
        store.resources.push_back(std::move(subjectStr));
        store.resources.push_back(std::move(predicateStr));
        return SERD_SUCCESS;
    }
    auto objectStr = reader->getAsString(object);
    if (subjectStr.empty() || predicateStr.empty() || objectStr.empty()) { // skip empty iris.
        return SERD_SUCCESS;
    }
    // Add subject, predicate and object as resource.
    store.resources.push_back(std::move(subjectStr));
    store.resources.push_back(std::move(predicateStr));
    store.resources.push_back(std::move(objectStr));
    return SERD_SUCCESS;
}

uint64_t RdfResourceReader::readToVector(DataChunk* dataChunk) {
    auto& rVector = dataChunk->getValueVectorMutable(0);
    auto& store = ku_dynamic_cast<ResourceStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(&rVector, i, store.resources[cursor + i]);
    }
    return numTuplesToScan;
}

SerdStatus RdfLiteralReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode* object_lang) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = reader->store_->cast<LiteralStore>();
    if (object->type != SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    auto subjectStr = reader->getAsString(subject);
    auto predicateStr = reader->getAsString(predicate);
    if (subjectStr.empty() || predicateStr.empty()) { // skip empty iris.
        return SERD_SUCCESS;
    }
    auto objectStr = reader->getAsString(object);
    store.literals.push_back(std::move(objectStr));
    if (object_datatype != nullptr) {
        auto objectTypeStr = reader->getAsString(object_datatype);
        auto typeID = RdfUtils::getLogicalTypeID(objectTypeStr);
        store.literalTypes.push_back(typeID);
    } else {
        store.literalTypes.push_back(LogicalTypeID::STRING);
    }
    if (object_lang != nullptr) {
        auto langStr = reader->getAsString(object_lang);
        store.langs.push_back(std::move(langStr));
    } else {
        store.langs.push_back("");
    }
    return SERD_SUCCESS;
}

uint64_t RdfLiteralReader::readToVector(common::DataChunk* dataChunk) {
    auto& lVector = dataChunk->getValueVectorMutable(0);
    auto& langVector = dataChunk->getValueVectorMutable(1);
    auto& store = store_->cast<LiteralStore>();
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        auto& literal = store.literals[cursor + i];
        auto& type = store.literalTypes[cursor + i];
        auto& lang = store.langs[cursor + i];
        RdfUtils::addRdfLiteral(&lVector, i, literal, type);
        StringVector::addString(&langVector, i, lang);
    }
    numLiteralTriplesScanned += numTuplesToScan;
    return numTuplesToScan;
}

SerdStatus RdfResourceTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = reader->store_->cast<ResourceTripleStore>();
    if (object->type == SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    auto subjectStr = reader->getAsString(subject);
    auto predicateStr = reader->getAsString(predicate);
    auto objectStr = reader->getAsString(object);
    if (subjectStr.empty() || predicateStr.empty() || objectStr.empty()) { // skip empty iris.
        return SERD_SUCCESS;
    }
    store.subjects.push_back(std::move(subjectStr));
    store.predicates.push_back(std::move(predicateStr));
    store.objects.push_back(std::move(objectStr));
    return SERD_SUCCESS;
}

uint64_t RdfResourceTripleReader::readToVector(DataChunk* dataChunk) {
    auto& sVector = dataChunk->getValueVectorMutable(0);
    auto& pVector = dataChunk->getValueVectorMutable(1);
    auto& oVector = dataChunk->getValueVectorMutable(2);
    auto& store = store_->cast<ResourceTripleStore>();
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(&sVector, i, store.subjects[cursor + i]);
        StringVector::addString(&pVector, i, store.predicates[cursor + i]);
        StringVector::addString(&oVector, i, store.objects[cursor + i]);
    }
    return numTuplesToScan;
}

SerdStatus RdfLiteralTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = reader->store_->cast<LiteralTripleStore>();
    if (object->type != SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    auto subjectStr = reader->getAsString(subject);
    auto predicateStr = reader->getAsString(predicate);
    if (subjectStr.empty() || predicateStr.empty()) { // skip empty iris.
        return SERD_SUCCESS;
    }
    store.subjects.push_back(std::move(subjectStr));
    store.predicates.push_back(std::move(predicateStr));
    return SERD_SUCCESS;
}

uint64_t RdfLiteralTripleReader::readToVector(DataChunk* dataChunk) {
    auto& sVector = dataChunk->getValueVectorMutable(0);
    auto& pVector = dataChunk->getValueVectorMutable(1);
    auto& oVector = dataChunk->getValueVectorMutable(2);
    auto& store = store_->cast<LiteralTripleStore>();
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(&sVector, i, store.subjects[cursor + i]);
        StringVector::addString(&pVector, i, store.predicates[cursor + i]);
        oVector.setValue(i, startOffset + numLiteralTriplesScanned + i);
    }
    numLiteralTriplesScanned += numTuplesToScan;
    return numTuplesToScan;
}

SerdStatus RdfTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode* object_lang) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = reader->store_->cast<TripleStore>();
    auto subjectStr = reader->getAsString(subject);
    auto predicateStr = reader->getAsString(predicate);
    auto objectStr = reader->getAsString(object);
    if (object->type == SERD_LITERAL) {
        if (subjectStr.empty() || predicateStr.empty()) { // skip empty iris.
            return SERD_SUCCESS;
        }
        store.ltStore.subjects.push_back(std::move(subjectStr));
        store.ltStore.predicates.push_back(std::move(predicateStr));
        store.ltStore.objects.push_back(std::move(objectStr));
        if (object_datatype != nullptr) {
            auto objectTypeStr = reader->getAsString(object_datatype);
            auto typeID = RdfUtils::getLogicalTypeID(objectTypeStr);
            store.ltStore.objectTypes.push_back(typeID);
        } else {
            store.ltStore.objectTypes.push_back(LogicalTypeID::STRING);
        }
        if (object_lang != nullptr) {
            auto langStr = reader->getAsString(object_lang);
            store.ltStore.langs.push_back(std::move(langStr));
        } else {
            store.ltStore.langs.push_back("");
        }
        return SERD_SUCCESS;
    }
    if (subjectStr.empty() || predicateStr.empty() || objectStr.empty()) { // skip empty iris.
        return SERD_SUCCESS;
    }
    store.rtStore.subjects.push_back(std::move(subjectStr));
    store.rtStore.predicates.push_back(std::move(predicateStr));
    store.rtStore.objects.push_back(std::move(objectStr));
    return SERD_SUCCESS;
}

} // namespace processor
} // namespace kuzu
