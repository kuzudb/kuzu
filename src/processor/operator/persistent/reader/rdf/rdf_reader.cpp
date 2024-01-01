#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
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
    default:
        KU_UNREACHABLE;
    }
}

void RdfReader::initReader() {
    KU_ASSERT(reader == nullptr);
    fp = fopen(this->filePath.c_str(), "rb");
    SerdStatementSink statementHandle;
    switch (mode) {
    case RdfReaderMode::RESOURCE: {
        statementHandle = rHandle;
    } break;
    case RdfReaderMode::LITERAL: {
        statementHandle = lHandle;
    } break;
    case RdfReaderMode::RESOURCE_TRIPLE: {
        statementHandle = rrrHandle;
    } break;
    case RdfReaderMode::LITERAL_TRIPLE: {
        statementHandle = rrlHandle;
    } break;
    case RdfReaderMode::ALL: {
        statementHandle = allHandle;
    } break;
    default:
        KU_UNREACHABLE;
    }
    reader = serd_reader_new(
        getSerdSyntax(fileType), this, nullptr, nullptr, prefixHandle, statementHandle, nullptr);
    serd_reader_set_strict(reader, false /* strict */);
    serd_reader_set_error_sink(reader, errorHandle, this);
    auto fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
}

SerdStatus RdfReader::errorHandle(void* /*handle*/, const SerdError* error) {
    if (error->status == SERD_ERR_BAD_SYNTAX) {
        return error->status;
    }
    if (error->status != SERD_SUCCESS && error->status != SERD_FAILURE) {
        throw common::CopyException(
            common::stringFormat("{} while reading rdf file at line {} and col {}",
                (char*)serd_strerror(error->status), error->line, error->col));
    }
    return error->status;
}

static bool supportSerdType(SerdType type) {
    switch (type) {
    case SERD_URI:
    case SERD_CURIE:
    case SERD_LITERAL:
        return true;
    case SERD_BLANK:
        return false;
    default:
        KU_UNREACHABLE;
    }
}

static bool supportTriple(
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object) {
    return supportSerdType(subject->type) && supportSerdType(predicate->type) &&
           supportSerdType(object->type);
}

static void addResourceToVector(
    common::ValueVector* vector, uint32_t vectorPos, const SerdNode* resourceNode) {
    StringVector::addString(
        vector, vectorPos, reinterpret_cast<const char*>(resourceNode->buf), resourceNode->n_bytes);
}

static void addLiteralToVector(common::ValueVector* vector, uint32_t pos,
    const SerdNode* literalNode, const SerdNode* typeNode) {
    if (typeNode == nullptr) { // No type information available. Treat as string.
        RdfUtils::addRdfLiteral(vector, pos, (const char*)literalNode->buf, literalNode->n_bytes);
        return;
    }
    RdfUtils::addRdfLiteral(vector, pos, (const char*)literalNode->buf, literalNode->n_bytes,
        (const char*)typeNode->buf, typeNode->n_bytes);
}

SerdStatus RdfReader::rHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto sVector = reader->dataChunk->getValueVector(0).get();
    auto& selectedSize = reader->dataChunk->state->selVector->selectedSize;
    addResourceToVector(sVector, selectedSize++, subject);
    addResourceToVector(sVector, selectedSize++, predicate);
    if (object->type != SERD_LITERAL) {
        addResourceToVector(sVector, selectedSize++, object);
    }
    return SERD_SUCCESS;
}

SerdStatus RdfReader::lHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto lVector = reader->dataChunk->getValueVector(0).get();
    if (object->type != SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    auto& selectedSize = reader->dataChunk->state->selVector->selectedSize;
    addLiteralToVector(lVector, selectedSize++, object, object_datatype);
    reader->rowOffset++;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::rrrHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (object->type == SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    auto sVector = reader->dataChunk->getValueVector(0).get();
    auto pVector = reader->dataChunk->getValueVector(1).get();
    auto oVector = reader->dataChunk->getValueVector(2).get();
    auto& selectedSize = reader->dataChunk->state->selVector->selectedSize;
    addResourceToVector(sVector, selectedSize, subject);
    addResourceToVector(pVector, selectedSize, predicate);
    addResourceToVector(oVector, selectedSize, object);
    selectedSize++;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::rrlHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportSerdType(subject->type) || !supportSerdType(predicate->type) ||
        !supportSerdType(object->type)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (object->type != SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    auto sVector = reader->dataChunk->getValueVector(0).get();
    auto pVector = reader->dataChunk->getValueVector(1).get();
    auto oVector = reader->dataChunk->getValueVector(2).get();
    auto& selectedSize = reader->dataChunk->state->selVector->selectedSize;
    addResourceToVector(sVector, selectedSize, subject);
    addResourceToVector(pVector, selectedSize, predicate);
    oVector->setValue<int64_t>(selectedSize, reader->rowOffset);
    selectedSize++;
    reader->rowOffset++;
    return SERD_SUCCESS;
}

static void addNode(std::vector<std::string>& vector, const SerdNode* node) {
    vector.emplace_back((const char*)node->buf, node->n_bytes);
}

SerdStatus RdfReader::allHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (object->type == SERD_LITERAL) {
        addNode(reader->store->literalTripleStore.subjects, subject);
        addNode(reader->store->literalTripleStore.predicates, predicate);
        addNode(reader->store->literalTripleStore.objects, object);
        if (object_datatype != nullptr) {
            addNode(reader->store->literalTripleStore.objectType, object_datatype);
        } else {
            reader->store->literalTripleStore.objectType.push_back("");
        }
    } else {
        addNode(reader->store->resourceTripleStore.subjects, subject);
        addNode(reader->store->resourceTripleStore.predicates, predicate);
        addNode(reader->store->resourceTripleStore.objects, object);
    }
    return SERD_SUCCESS;
}

SerdStatus RdfReader::prefixHandle(void* handle, const SerdNode* /*name*/, const SerdNode* uri) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    reader->currentPrefix = reinterpret_cast<const char*>(uri->buf);
    return SERD_SUCCESS;
}

offset_t RdfReader::read(DataChunk* dataChunk_) {
    if (status) {
        return 0;
    }
    dataChunk = dataChunk_;
    while (true) {
        status = serd_reader_read_chunk(reader);
        // See comment below.
        if (dataChunk->state->selVector->selectedSize > DEFAULT_VECTOR_CAPACITY) {
            throw RuntimeException("Vector size exceed DEFAULT_VECTOR_CAPACITY.");
        }
        if (status == SERD_ERR_BAD_SYNTAX) {
            // Skip to the next line.
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            continue;
        }
        // We cannot control how many rows being read in each serd_reader_read_chunk(). Empirically,
        // a chunk shouldn't be too large. We leave a buffer size 100 to avoid vector size exceed
        // 2048 after reading the last chunk.
        if (status != SERD_SUCCESS ||
            dataChunk->state->selVector->selectedSize + 100 >= DEFAULT_VECTOR_CAPACITY) {
            break;
        }
    }
    return dataChunk->state->selVector->selectedSize;
}

} // namespace processor
} // namespace kuzu
