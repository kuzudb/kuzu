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

void RdfReader::initInternal(SerdStatementSink statementSink) {
    KU_ASSERT(reader == nullptr);
    fp = fopen(this->filePath.c_str(), "rb");
    reader = serd_reader_new(
        getSerdSyntax(fileType), this, nullptr, nullptr, prefixHandle, statementSink, nullptr);
    serd_reader_set_strict(reader, false /* strict */);
    serd_reader_set_error_sink(reader, errorHandle, this);
    auto fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
}

SerdStatus RdfReader::errorHandle(void*, const SerdError* error) {
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

static void addNode(std::vector<std::string>& vector, const SerdNode* node) {
    vector.emplace_back((const char*)node->buf, node->n_bytes);
}

SerdStatus RdfReader::prefixHandle(void* handle, const SerdNode* /*name*/, const SerdNode* uri) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    reader->currentPrefix = reinterpret_cast<const char*>(uri->buf);
    return SERD_SUCCESS;
}

offset_t RdfReader::readChunk(DataChunk* dataChunk) {
    if (cursor == store_->size()) {
        cursor = 0;
        store_->clear();
        while (true) {
            if (status != SERD_SUCCESS || store_->size() > DEFAULT_VECTOR_CAPACITY) {
                break;
            }
            status = serd_reader_read_chunk(reader);
            if (status == SERD_ERR_BAD_SYNTAX) {
                // Skip to the next line.
                serd_reader_skip_until_byte(reader, (uint8_t)'\n');
                status = SERD_SUCCESS;
                continue;
            }
        }
    }
    if (store_->isEmpty()) {
        return 0;
    }
    auto numTuplesRead = readToVector(dataChunk);
    cursor += numTuplesRead;
    dataChunk->state->selVector->selectedSize = numTuplesRead;
    return numTuplesRead;
}

void RdfReader::readAll() {
    if (status) {
        return;
    }
    while (true) {
        status = serd_reader_read_chunk(reader);
        if (status == SERD_ERR_BAD_SYNTAX) {
            // Skip to the next line.
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            continue;
        }
        if (status != SERD_SUCCESS) {
            return;
        }
    }
}

SerdStatus RdfResourceReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, ResourceStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        addNode(store.resources, subject);
        addNode(store.resources, predicate);
    } else {
        addNode(store.resources, subject);
        addNode(store.resources, predicate);
        addNode(store.resources, object);
    }
    return SERD_SUCCESS;
}

uint64_t RdfResourceReader::readToVector(DataChunk* dataChunk) {
    auto rVector = dataChunk->getValueVector(0).get();
    auto& store = ku_dynamic_cast<RdfStore&, ResourceStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(rVector, i, store.resources[cursor + i]);
    }
    return numTuplesToScan;
}

SerdStatus RdfLiteralReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, LiteralStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        addNode(store.literals, object);
        if (object_datatype != nullptr) {
            addNode(store.literalTypes, object_datatype);
        } else {
            store.literalTypes.push_back("");
        }
    }
    return SERD_SUCCESS;
}

uint64_t RdfLiteralReader::readToVector(common::DataChunk* dataChunk) {
    auto lVector = dataChunk->getValueVector(0).get();
    auto& store = ku_dynamic_cast<RdfStore&, LiteralStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        auto& literal = store.literals[cursor + i];
        auto& type = store.literalTypes[cursor + i];
        if (type.empty()) {
            RdfVariantVector::addString(lVector, i, literal.data(), literal.size());
        } else {
            RdfUtils::addRdfLiteral(
                lVector, i, literal.data(), literal.size(), type.data(), type.size());
        }
    }
    return numTuplesToScan;
}

SerdStatus RdfResourceTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, ResourceTripleStore&>(*reader->store_);
    if (object->type != SERD_LITERAL) {
        addNode(store.subjects, subject);
        addNode(store.predicates, predicate);
        addNode(store.objects, object);
    }
    return SERD_SUCCESS;
}

uint64_t RdfResourceTripleReader::readToVector(DataChunk* dataChunk) {
    auto sVector = dataChunk->getValueVector(0).get();
    auto pVector = dataChunk->getValueVector(1).get();
    auto oVector = dataChunk->getValueVector(2).get();
    auto& store = ku_dynamic_cast<RdfStore&, ResourceTripleStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, store.subjects[cursor + i]);
        StringVector::addString(pVector, i, store.predicates[cursor + i]);
        StringVector::addString(oVector, i, store.objects[cursor + i]);
    }
    return numTuplesToScan;
}

SerdStatus RdfLiteralTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, LiteralTripleStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        addNode(store.subjects, subject);
        addNode(store.predicates, predicate);
    }
    return SERD_SUCCESS;
}

uint64_t RdfLiteralTripleReader::readToVector(DataChunk* dataChunk) {
    auto sVector = dataChunk->getValueVector(0).get();
    auto pVector = dataChunk->getValueVector(1).get();
    auto oVector = dataChunk->getValueVector(2).get();
    auto& store = ku_dynamic_cast<RdfStore&, LiteralTripleStore&>(*store_);
    auto numTuplesToScan = std::min(store.size() - cursor, DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < numTuplesToScan; ++i) {
        StringVector::addString(sVector, i, store.subjects[cursor + i]);
        StringVector::addString(pVector, i, store.predicates[cursor + i]);
        oVector->setValue(i, rowOffset + i);
    }
    rowOffset += numTuplesToScan;
    return numTuplesToScan;
}

SerdStatus RdfTripleReader::handle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    auto& store = ku_dynamic_cast<RdfStore&, TripleStore&>(*reader->store_);
    if (object->type == SERD_LITERAL) {
        addNode(store.ltStore.subjects, subject);
        addNode(store.ltStore.predicates, predicate);
        addNode(store.ltStore.objects, object);
        if (object_datatype != nullptr) {
            addNode(store.ltStore.objectTypes, object_datatype);
        } else {
            store.ltStore.objectTypes.push_back("");
        }
    } else {
        addNode(store.rtStore.subjects, subject);
        addNode(store.rtStore.predicates, predicate);
        addNode(store.rtStore.objects, object);
    }
    return SERD_SUCCESS;
}

} // namespace processor
} // namespace kuzu
