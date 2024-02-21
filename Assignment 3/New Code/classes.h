/*
Skeleton code for storage and buffer management

Member 1
Name : Shreeya Badhe
OSU email :badhes@oregonstate.edu
ONID :badhes


Member 2
Name : Rahil Piyush Mehta
OSU email : mehtara@oregonstate.edu
ONID : mehtara

*/
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <bitset>
#include <fstream>
#include <cmath>
#include <cstdio>
#include <cstdint>

using namespace std;

class Record {
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields) {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }


    int calcSize() {
        return name.length() + bio.length() + 8 + 8  + 4;

    }


    void writeSerializeRecord(fstream &indexFile) {

        int64_t EmployeeId = (int64_t)id;
        int64_t ManagerId = (int64_t)manager_id;

        indexFile.write(reinterpret_cast<const char *>(&EmployeeId), sizeof(EmployeeId));
        indexFile << "|";
        indexFile << name;
        indexFile << "|";
        indexFile << bio;
        indexFile << "|";
        indexFile.write(reinterpret_cast<const char *>(&ManagerId), sizeof(ManagerId));
        indexFile << "|";

    }

};


class DataBlock {
private:
    const int PAGE_SIZE = 4096;

public:

    vector<Record> blockRecords;
    int blockSize;
    int nextBlockIndex;
    int physicalIndex;
    int recordCount;



    void readBlock(fstream &inputFile) {
        inputFile.seekg(physicalIndex * PAGE_SIZE);
        inputFile.read(reinterpret_cast<char *>(&nextBlockIndex), sizeof(nextBlockIndex));
        inputFile.read(reinterpret_cast<char *>(&recordCount), sizeof(recordCount));
        blockSize += 8;

        for (int i = 0; i < recordCount; i++) {
            readRecord(inputFile);
            blockSize += blockRecords[i].calcSize();

        }
    }

    void readRecord(fstream &inputFile) {
        int64_t Id;
        int64_t ManagerId;
        string name, bio;
        inputFile.read(reinterpret_cast<char *>(&Id), sizeof(Id));
        inputFile.ignore(1, '|');
        getline(inputFile, name, '|');
        getline(inputFile, bio, '|');
        inputFile.read(reinterpret_cast<char *>(&ManagerId), sizeof(ManagerId));
        inputFile.ignore(1, '|');
        int regularId = (int)Id;
        int regularManagerId = (int)ManagerId;
        vector<string> recordData{to_string(regularId), name, bio, to_string(regularManagerId)};

        blockRecords.push_back(Record(recordData));

    }
    DataBlock() {
        blockSize = 0;
        physicalIndex = 0;
    }

    DataBlock(int physIdx) {
        blockSize = 0;
        physicalIndex = physIdx;
    }

};


class LinearHashIndex {

private:
    const int BLOCK_SIZE = 4096;

    vector<int> blockDirectory;
    int numBlocks;
    int numBuckets;
    int i;
    int numRecords;
    int nextFreeBlock;
    string fName;
    int overflowBlockCount;

    int currentTotalBlockSize;

    int hashFunction(int id) {
        return (id % (int)pow(2, 16));
    }

    int getLastTHBits(int hashVal, int i) {
        return hashVal & ((1 << i) - 1);
    }

    int initializeNewBucket(fstream &indexFile) {

        int noOverflowPointer = -1;
        int initialRecordCount = 0;

        indexFile.seekp(nextFreeBlock * BLOCK_SIZE);
        indexFile.write(reinterpret_cast<const char *>(&noOverflowPointer), sizeof(noOverflowPointer));
        indexFile.write(reinterpret_cast<const char *>(&initialRecordCount), sizeof(initialRecordCount));
        blockDirectory.push_back(nextFreeBlock++);
        numBlocks++;
        numBuckets++;
        currentTotalBlockSize += sizeof(noOverflowPointer) + sizeof(initialRecordCount);

        return blockDirectory.size() - 1;

    }

    int initializeOverflowBlock(int baseBlockIndex, fstream &dataFile) {

        int noNextOverflow = -1;
        int emptyRecordCount = 0;
        int currentBlockIndex = nextFreeBlock++;
        dataFile.seekp(currentBlockIndex * BLOCK_SIZE);
        dataFile.write(reinterpret_cast<const char *>(&noNextOverflow), sizeof(noNextOverflow));
        dataFile.write(reinterpret_cast<const char *>(&emptyRecordCount), sizeof(emptyRecordCount));
        dataFile.seekp(baseBlockIndex * BLOCK_SIZE);
        dataFile.write(reinterpret_cast<const char *>(&currentBlockIndex), sizeof(currentBlockIndex));
        currentTotalBlockSize += sizeof(noNextOverflow) + sizeof(emptyRecordCount);
        numBlocks++;
        overflowBlockCount++;

        return currentBlockIndex;

    }

    void writeRecordInIndexFile(Record record, int baseBlockPgIdx, fstream &indexFile) {

        bool recordStored = false;
        while (!recordStored) {
            DataBlock currentDataBlock(baseBlockPgIdx);
            currentDataBlock.readBlock(indexFile);
            if (currentDataBlock.blockSize + record.calcSize() <= BLOCK_SIZE) {

                indexFile.seekp((baseBlockPgIdx * BLOCK_SIZE) + currentDataBlock.blockSize);
                record.writeSerializeRecord(indexFile);
                int newNumRecords = currentDataBlock.recordCount + 1;

                indexFile.seekp((baseBlockPgIdx * BLOCK_SIZE) + sizeof(int));
                indexFile.write(reinterpret_cast<const char *>(&newNumRecords), sizeof(newNumRecords));
                currentTotalBlockSize += record.calcSize();
                recordStored = true;

            }
            else if (currentDataBlock.nextBlockIndex != -1) {
                baseBlockPgIdx = currentDataBlock.nextBlockIndex;

            }
            else {
                int newOverflowIndex = initializeOverflowBlock(baseBlockPgIdx, indexFile);
                indexFile.seekp((newOverflowIndex * BLOCK_SIZE) + sizeof(int) + sizeof(int));
                record.writeSerializeRecord(indexFile);
                int initialRecordCount = 1;
                indexFile.seekp((newOverflowIndex * BLOCK_SIZE) + sizeof(int));
                indexFile.write(reinterpret_cast<const char *>(&initialRecordCount), sizeof(initialRecordCount));
                currentTotalBlockSize += record.calcSize();
                recordStored = true;

            }

        }

    }

    int setupNewBlock(fstream &indexFile) {

        int noOverflowPointer = -1;
        int emptyRecordCount = 0;
        int newBlockIndex = nextFreeBlock++;
        indexFile.seekp(newBlockIndex * BLOCK_SIZE);
        indexFile.write(reinterpret_cast<const char *>(&noOverflowPointer), sizeof(noOverflowPointer));
        indexFile.write(reinterpret_cast<const char *>(&emptyRecordCount), sizeof(emptyRecordCount));
        currentTotalBlockSize += sizeof(noOverflowPointer) + sizeof(emptyRecordCount);
        numBlocks++;
        return newBlockIndex;

    }

    void insertRecord(Record record, fstream &indexFile) {
        if (numRecords == 0) {
            for (int i = 0; i < 2; i++) {
                initializeNewBucket(indexFile);
            }


            i = 1;

        }

        int bucketIndex = getLastTHBits(hashFunction(record.id), i);
        if (bucketIndex >= numBuckets) {
            bucketIndex &= ~(1 << (i-1));
        }

        int physicalPageIndex = blockDirectory[bucketIndex];
        writeRecordInIndexFile(record, physicalPageIndex, indexFile);

        numRecords++;

        double avgCapacityPerBucket = (double)currentTotalBlockSize / numBuckets;

        // 70% of block capacity is 70% of 4096
        if (avgCapacityPerBucket > 0.7 * BLOCK_SIZE) {

            int newBucketIndex = initializeNewBucket(indexFile);
            int digitsToAddrNewBucket = (int)ceil(log2(numBuckets));
            int realBucketIndexToMove = newBucketIndex;
            realBucketIndexToMove &= ~(1 << (digitsToAddrNewBucket - 1));
            int realBucketToMoveRecordsFromPgIdx = blockDirectory[realBucketIndexToMove];
            int newPhysicalPageIndexForOldBucket = setupNewBlock(indexFile);

            while (realBucketToMoveRecordsFromPgIdx != -1) {
                DataBlock PreviousBlock(realBucketToMoveRecordsFromPgIdx);
                PreviousBlock.readBlock(indexFile);

                indexFile.seekp(PreviousBlock.physicalIndex * BLOCK_SIZE);
                indexFile << string(BLOCK_SIZE, '*');

                numBlocks--;
                overflowBlockCount--;
                currentTotalBlockSize -= PreviousBlock.blockSize;
                numRecords -= PreviousBlock.recordCount;

                for (int i = 0; i < PreviousBlock.recordCount; i++) {
                    if (getLastTHBits(hashFunction(PreviousBlock.blockRecords[i].id), digitsToAddrNewBucket) == newBucketIndex) {
                        int newBucketBlockPageIdx = blockDirectory[newBucketIndex];
                        writeRecordInIndexFile(PreviousBlock.blockRecords[i], newBucketBlockPageIdx, indexFile);
                    }
                    else {

                        int tempNewOldBlockPageIdx = newPhysicalPageIndexForOldBucket;
                        writeRecordInIndexFile(PreviousBlock.blockRecords[i], tempNewOldBlockPageIdx, indexFile);

                    }

                    numRecords++;

                }
                realBucketToMoveRecordsFromPgIdx = PreviousBlock.nextBlockIndex;

            }
            overflowBlockCount++;
            blockDirectory[realBucketIndexToMove] = newPhysicalPageIndexForOldBucket;
            i = digitsToAddrNewBucket;

        }

    }

    Record getRecord(fstream &recordIn) {

        string line, cell;

        // Make vector of strings (fields of record)
        // to pass to constructor of Record struct
        vector<std::string> fields;

        // grab entire line
        if (getline(recordIn, line, '\n'))
        {
            stringstream lineStream(line);
            while (getline(lineStream, cell, ',')) {
                fields.push_back(cell);
            }
            return Record(fields);

        }
        else
        {
            // Put error indicator in first field
            fields.push_back("-1");
            fields.push_back("-1");
            fields.push_back("-1");
            fields.push_back("-1");
            return Record(fields);
        }

    }


public:
    LinearHashIndex(string indexFileName) {
        numBlocks = 0;
        i = 0;
        numRecords = 0;
        numBuckets = 0;
        fName = indexFileName;
        overflowBlockCount = 0;
        currentTotalBlockSize = 0;
        nextFreeBlock = 0;
    }

    void createFromFile(string csvFName) {
        fstream dataFile(fName, ios::in | ios::out | ios::trunc | ios::binary);
        fstream inputFile(csvFName, ios::in);

        if (!inputFile) {
            cout << "Failed to open " << csvFName << endl;
            return;
        }
        if (!dataFile) {
            cout << "Failed to create or open " << fName << " for writing." << endl;
            return;
        }

        bool recordsRemaining = true;

        while (recordsRemaining) {

            Record singleRec = getRecord(inputFile);
            if (singleRec.id == -1) {
                recordsRemaining = false;
            }
            else {
                insertRecord(singleRec, dataFile);
            }

        }
        dataFile.close();
        inputFile.close();
    }

    Record findRecordById(int id) {
        fstream indexFile(fName, ios::in);
        int bucketIndex = getLastTHBits(hashFunction(id), i);
        if (bucketIndex >= numBuckets) {
            bucketIndex &= ~(1 << (i-1));
        }

        int physicalPageIndex = blockDirectory[bucketIndex];

        while (physicalPageIndex != -1) {
            DataBlock currBlock(physicalPageIndex);
            currBlock.readBlock(indexFile);

            for (int i = 0; i < currBlock.recordCount; i++) {

                // Return record if found
                if(currBlock.blockRecords[i].id == id)
                    return currBlock.blockRecords[i];

            }
            physicalPageIndex = currBlock.nextBlockIndex;
        }

        indexFile.close();
    }
};