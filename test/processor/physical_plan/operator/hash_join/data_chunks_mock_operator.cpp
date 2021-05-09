#include "data_chunks_mock_operator.h"

void ScanMockOp::getNextTuples() {
    lock_guard<mutex> lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        resultSet->dataChunks[1]->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    for (auto i = 0u; i < 3; i++) {
        resultSet->dataChunks[i]->state->size = 0;
        resultSet->dataChunks[i]->state->numSelectedValues = 0;
    }
}

void ScanMockOp::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme, false);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme, false);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme, false);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        ((uint64_t*)vectorA1->values)[i] = (uint64_t)i;
        ((int32_t*)vectorA1->values)[i] = (int32_t)(i * 2);
        ((uint64_t*)vectorB1->values)[i] = (uint64_t)i;
        ((double_t*)vectorB2->values)[i] = (double_t)(i / 2);
        ((uint64_t*)vectorC1->values)[i] = (uint64_t)i;
        ((uint8_t*)vectorC2->values)[i] = (bool)((i / 2) == 1);
    }
    dataChunkA->append(vectorA1);
    dataChunkA->append(vectorA2);
    dataChunkB->append(vectorB1);
    dataChunkB->append(vectorB2);
    dataChunkC->append(vectorC1);
    dataChunkC->append(vectorC2);

    dataChunkA->state->size = 10;
    dataChunkB->state->size = 10;
    dataChunkC->state->size = 10;
    dataChunkA->state->numSelectedValues = 10;
    dataChunkB->state->numSelectedValues = 10;
    dataChunkC->state->numSelectedValues = 10;
    dataChunkA->state->currPos = 0;
    dataChunkB->state->currPos = 0;
    dataChunkC->state->currPos = -1;
    resultSet->append(dataChunkA);
    resultSet->append(dataChunkB);
    resultSet->append(dataChunkC);
}

void ScanMockOpWithSelector::getNextTuples() {
    lock_guard<mutex> lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        resultSet->dataChunks[1]->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    for (auto i = 0u; i < 3; i++) {
        resultSet->dataChunks[i]->state->size = 0;
        resultSet->dataChunks[i]->state->numSelectedValues = 0;
    }
}

void ScanMockOpWithSelector::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme, false);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme, false);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme, false);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        ((uint64_t*)vectorA1->values)[i] = (uint64_t)i;
        ((int32_t*)vectorA1->values)[i] = (int32_t)(i * 2);
        ((uint64_t*)vectorB1->values)[i] = (uint64_t)i;
        ((double_t*)vectorB2->values)[i] = (double_t)(i / 2);
        ((uint64_t*)vectorC1->values)[i] = (uint64_t)i;
        vectorC2->values[i] = (bool)((i / 2) == 1);
    }
    dataChunkA->append(vectorA1);
    dataChunkA->append(vectorA2);
    dataChunkB->append(vectorB1);
    dataChunkB->append(vectorB2);
    dataChunkC->append(vectorC1);
    dataChunkC->append(vectorC2);

    dataChunkA->state->size = 10;
    dataChunkB->state->size = 10;
    dataChunkC->state->size = 10;
    dataChunkA->state->numSelectedValues = 10;
    dataChunkB->state->numSelectedValues = 10;
    dataChunkC->state->numSelectedValues = 10;
    for (auto i = 0u; i < 10; i++) {
        dataChunkB->state->selectedValuesPos[i] = 2;
    }
    dataChunkA->state->currPos = 0;
    dataChunkB->state->currPos = 0;
    dataChunkC->state->currPos = -1;
    resultSet->append(dataChunkA);
    resultSet->append(dataChunkB);
    resultSet->append(dataChunkC);
}

void ProbeScanMockOp::getNextTuples() {
    lock_guard<mutex> lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        resultSet->dataChunks[1]->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    for (auto i = 0u; i < 3; i++) {
        resultSet->dataChunks[i]->state->size = 0;
        resultSet->dataChunks[i]->state->numSelectedValues = 0;
    }
}

void ProbeScanMockOp::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme, false);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme, false);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme, false);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        ((uint64_t*)vectorA1->values)[i] = (uint64_t)i;
        ((int32_t*)vectorA1->values)[i] = (int32_t)(i * 2);
        ((uint64_t*)vectorB1->values)[i] = (uint64_t)i;
        ((double_t*)vectorB2->values)[i] = (double_t)(i / 2);
        ((uint64_t*)vectorC1->values)[i] = (uint64_t)i;
        vectorC2->values[i] = (bool)((i / 2) == 1);
    }
    dataChunkA->append(vectorA1);
    dataChunkA->append(vectorA2);
    dataChunkB->append(vectorB1);
    dataChunkB->append(vectorB2);
    dataChunkC->append(vectorC1);
    dataChunkC->append(vectorC2);

    dataChunkA->state->size = 10;
    dataChunkB->state->size = 10;
    dataChunkC->state->size = 10;
    dataChunkA->state->numSelectedValues = 10;
    dataChunkB->state->numSelectedValues = 10;
    dataChunkC->state->numSelectedValues = 10;
    dataChunkA->state->currPos = 0;
    dataChunkB->state->currPos = 2;
    dataChunkC->state->currPos = -1;
    resultSet->append(dataChunkA);
    resultSet->append(dataChunkB);
    resultSet->append(dataChunkC);
}

void ProbeScanMockOpWithSelector::getNextTuples() {
    lock_guard<mutex> lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        resultSet->dataChunks[1]->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    for (auto i = 0u; i < 3; i++) {
        resultSet->dataChunks[i]->state->size = 0;
        resultSet->dataChunks[i]->state->numSelectedValues = 0;
    }
}

void ProbeScanMockOpWithSelector::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme, false);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme, false);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme, false);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        ((uint64_t*)vectorA1->values)[i] = (uint64_t)i;
        ((int32_t*)vectorA1->values)[i] = (int32_t)(i * 2);
        ((uint64_t*)vectorB1->values)[i] = (uint64_t)i;
        ((double_t*)vectorB2->values)[i] = (double_t)(i / 2);
        ((uint64_t*)vectorC1->values)[i] = (uint64_t)i;
        vectorC2->values[i] = (bool)((i / 2) == 1);
    }
    dataChunkA->append(vectorA1);
    dataChunkA->append(vectorA2);
    dataChunkB->append(vectorB1);
    dataChunkB->append(vectorB2);
    dataChunkC->append(vectorC1);
    dataChunkC->append(vectorC2);

    dataChunkA->state->size = 10;
    dataChunkB->state->size = 10;
    dataChunkC->state->size = 10;
    dataChunkA->state->selectedValuesPos[0] = 0;
    dataChunkB->state->selectedValuesPos[0] = 2;
    dataChunkC->state->selectedValuesPos[0] = 3;
    dataChunkA->state->numSelectedValues = 1;
    dataChunkB->state->numSelectedValues = 1;
    dataChunkC->state->numSelectedValues = 1;
    dataChunkA->state->currPos = 0;
    dataChunkB->state->currPos = 0;
    dataChunkC->state->currPos = -1;
    resultSet->append(dataChunkA);
    resultSet->append(dataChunkB);
    resultSet->append(dataChunkC);
}
