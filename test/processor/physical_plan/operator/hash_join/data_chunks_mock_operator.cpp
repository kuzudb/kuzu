#include "data_chunks_mock_operator.h"
/*
void ScanMockOp::getNextTuples() {
    lock_guard lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        dataChunks->getDataChunkState(1)->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    dataChunks->getDataChunkState(0)->size = 0;
    dataChunks->getDataChunkState(1)->size = 0;
    dataChunks->getDataChunkState(2)->size = 0;
    dataChunks->getDataChunkState(0)->numSelectedValues = 0;
    dataChunks->getDataChunkState(1)->numSelectedValues = 0;
    dataChunks->getDataChunkState(2)->numSelectedValues = 0;
}

void ScanMockOp::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
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
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}

void ScanMockOpWithSelector::getNextTuples() {
    lock_guard lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        dataChunks->getDataChunk(1)->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    dataChunks->getDataChunkState(0)->size = 0;
    dataChunks->getDataChunkState(1)->size = 0;
    dataChunks->getDataChunkState(2)->size = 0;
    dataChunks->getDataChunkState(0)->numSelectedValues = 0;
    dataChunks->getDataChunkState(1)->numSelectedValues = 0;
    dataChunks->getDataChunkState(2)->numSelectedValues = 0;
}

void ScanMockOpWithSelector::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
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
    for (uint64_t i = 0; i < 10; i++) {
        dataChunkB->state->selectedValuesPos[i] = 2;
    }
    dataChunkA->state->currPos = 0;
    dataChunkB->state->currPos = 0;
    dataChunkC->state->currPos = -1;
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}

void ProbeScanMockOp::getNextTuples() {
    lock_guard lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        dataChunks->getDataChunk(1)->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    dataChunks->getDataChunkState(0)->size = 0;
    dataChunks->getDataChunkState(1)->size = 0;
    dataChunks->getDataChunkState(2)->size = 0;
    dataChunks->getDataChunkState(0)->numSelectedValues = 0;
    dataChunks->getDataChunkState(1)->numSelectedValues = 0;
    dataChunks->getDataChunkState(2)->numSelectedValues = 0;
}

void ProbeScanMockOp::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
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
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}

void ProbeScanMockOpWithSelector::getNextTuples() {
    lock_guard lock(morsel.mtx);
    if (morsel.currNodeOffset < morsel.numNodes) {
        dataChunks->getDataChunk(1)->state->currPos = morsel.currNodeOffset;
        morsel.currNodeOffset += 1;
        return;
    }
    dataChunks->getDataChunkState(0)->size = 0;
    dataChunks->getDataChunkState(1)->size = 0;
    dataChunks->getDataChunkState(2)->size = 0;
    dataChunks->getDataChunkState(0)->numSelectedValues = 0;
    dataChunks->getDataChunkState(1)->numSelectedValues = 0;
    dataChunks->getDataChunkState(2)->numSelectedValues = 0;
}

void ProbeScanMockOpWithSelector::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
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
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}
*/
