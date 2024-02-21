#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
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

    void writeSerializeRecord(fstream &indexFile) {

        int64_t EmployeeId = static_cast<int64_t>(id);
        int64_t ManagerId = static_cast<int64_t>(manager_id);

        // Writing the extended (padded) IDs and textual information to the data file
        indexFile.write(reinterpret_cast<const char *>(&EmployeeId), sizeof(EmployeeId));
        indexFile.put('|'); // Using a different delimiter for variety
        indexFile << name;
        indexFile.put('|');
        indexFile << bio;
        indexFile.put('|');
        indexFile.write(reinterpret_cast<const char *>(&ManagerId), sizeof(ManagerId));
        indexFile.put('|');
    }

    int calcSize() {
        return name.length()+bio.length()  +8 + 8    + 4;
    }
};

class DataBlock {
private:
    const int BlockCapacity = 4096; // Renamed PAGE_SIZE to BlockCapacity for clarity

public:

    vector<Record> blockRecords; // Renamed records to blockRecords
    int utilizedSpace; // Renamed blockSize to utilizedSpace

    // Index of the next block in case of overflow
    int nextBlockIndex; // Renamed overflowPtrIdx to nextBlockIndex

    // The count of records present in this block
    int recordCount; // Renamed numRecords to recordCount

    // The position of this block within the storage file
    int physicalIndex; // Renamed blockIdx to physicalIndex


    DataBlock() : utilizedSpace(0), physicalIndex(0), nextBlockIndex(-1), recordCount(0) {}

    // Constructor with specified physical index
    DataBlock(int index) : utilizedSpace(0), physicalIndex(index), nextBlockIndex(-1), recordCount(0) {}

    // Method to load a single record from the storage file
    void loadRecordFromStorage(fstream &storageFile) {
        int64_t Id, ManagerId;
        string Name, Bio;

        storageFile.read(reinterpret_cast<char *>(&Id), sizeof(Id));
        storageFile.ignore(1, '~'); // Skip delimiter
        getline(storageFile, Name, '~');
        getline(storageFile, Bio, '~');
        storageFile.read(reinterpret_cast<char *>(&ManagerId), sizeof(ManagerId));
        storageFile.ignore(1, '~'); // Skip delimiter

        // Convert 8-byte integers back to 4-byte integers
        int id = static_cast<int>(Id);
        int managerId = static_cast<int>(Id);
        vector<string> recordData = {to_string(id), Name, Bio, to_string(managerId)};

        blockRecords.push_back(Record(recordData));
    }

    // Method to populate this block with records from the storage file
    void populateBlock(fstream &storageFile) {
        storageFile.seekg(physicalIndex * BlockCapacity); // Navigate to the correct block
        storageFile.read(reinterpret_cast<char *>(&nextBlockIndex), sizeof(nextBlockIndex));
        storageFile.read(reinterpret_cast<char *>(&recordCount), sizeof(recordCount));
        utilizedSpace += sizeof(nextBlockIndex) + sizeof(recordCount); // Adjusting for metadata space

        for (int i = 0; i < recordCount; ++i) {
            loadRecordFromStorage(storageFile);
            utilizedSpace += blockRecords.back().calcSize(); // Update block's utilized space
        }
    }
};


class LinearHashIndex {

private:
    const int BLOCK_SIZE = 4096;

    vector<int> blockDirectory; // Map the least-significant-bits of h(id) to a bucket location in EmployeeIndex (e.g., the jth bucket)
                                // can scan to correct bucket using j*BLOCK_SIZE as offset (using seek function)
								// can initialize to a size of 256 (assume that we will never have more than 256 regular (i.e., non-overflow) buckets)
    int n;  // The number of indexes in blockDirectory currently being used
    int i;	// The number of least-significant-bits of h(id) to check. Will need to increase i once n > 2^i
    int numRecords;    // Records currently in index. Used to test whether to increase n
    int nextFreeBlock; // Next place to write a bucket. Should increment it by BLOCK_SIZE whenever a bucket is written to EmployeeIndex
    string fName;      // Name of index file

    int overflowBlockCount; // Renamed from numOverflowBlocks, count of overflow blocks created
    int totalBlockSize;
    int totalBlocks;

// Fetches a record from the input CSV file and converts it to a Record object
    Record extractRecord(fstream &inputStream) {
        string line, cell;
        vector<string> recordData; // Renamed from fields for clarity

        if (getline(inputStream, line)) {
            stringstream lineStream(line);
            while (getline(lineStream, cell, ',')) {
                recordData.push_back(cell);
            }
            return Record(recordData);
        } else {
            // Returning a default Record object indicating failure to read a valid record
            return Record(vector<string>{"-1", "N/A", "N/A", "-1"});
        }
    }


    int GethashValue(int id) {
        return (id % (int)pow(2, 16));
    }

    int getLastTHBits(int hashVal, int i) {
        return hashVal & ((1 << i) - 1);
    }

    int initializeNewBucket(fstream &dataFile) {
        int noOverflowPointer = -1;
        int initialRecordCount = 0;


        dataFile.seekp(nextFreeBlock * BLOCK_SIZE);


        dataFile.write(reinterpret_cast<const char *>(&noOverflowPointer), sizeof(noOverflowPointer));
        dataFile.write(reinterpret_cast<const char *>(&initialRecordCount), sizeof(initialRecordCount));

        // Update the blockDirectory with the new bucket's location
        blockDirectory.push_back(nextFreeBlock); // Renamed pageDirectory to blockDirectory
        nextFreeBlock++; // Increment the next free block pointer
        totalBlocks++; // Increment the total number of blocks, renamed from numBlocks
        n++; // Increment the number of buckets currently being used, renamed from numBuckets

        // Reflect the addition of a new bucket in the total size of data
        totalBlockSize += sizeof(noOverflowPointer) + sizeof(initialRecordCount); // Renamed currentTotalSize to totalBlockSize

        // Return the logical index of the new bucket within the block directory
        return blockDirectory.size() - 1;
    }

    int createOverflowBucket(int baseBlockIndex, fstream &dataFile) {
        int noNextOverflow = -1;
        int emptyRecordCount = 0;

        // Acquire the index for the new overflow block
        int currentBlockIndex = nextFreeBlock++;

        // Write initial block information (pointer to the next overflow block and record count)
        // These are placeholder values for a new overflow block
        dataFile.seekp(currentBlockIndex * BLOCK_SIZE);
        dataFile.write(reinterpret_cast<const char *>(&noNextOverflow), sizeof(noNextOverflow));
        dataFile.write(reinterpret_cast<const char *>(&emptyRecordCount), sizeof(emptyRecordCount));

        // Update the overflow pointer in the parent block to link to this new overflow block
        dataFile.seekp(baseBlockIndex * BLOCK_SIZE);
        dataFile.write(reinterpret_cast<const char *>(&currentBlockIndex), sizeof(currentBlockIndex));

        // Adjust the total size of data in the index to account for the new block's metadata
        totalBlockSize += sizeof(noNextOverflow) + sizeof(emptyRecordCount); // Renamed currentTotalSize to totalBlockSize for clarity

        // Reflect the addition of the new overflow block in the count of total and overflow blocks
        totalBlocks++;
        overflowBlockCount++;

        // Return the physical index of the new overflow block
        return currentBlockIndex;
    }


    // Initializes an empty bucket in the index file, returning the physical offset index of the new block
    int setupNewBlock(fstream &dataFile) {
        int noOverflowPointer = -1; // Clearer naming, indicating no overflow block is linked yet
        int emptyRecordCount = 0; // Indicates this new block initially contains no records

        // Determine the index for the next available block
        int newBlockIndex = nextFreeBlock++; // Renamed currIdx to newBlockIndex for clarity

        // Position the file pointer to the start of the new block and write the initial metadata
        dataFile.seekp(newBlockIndex * BLOCK_SIZE);
        dataFile.write(reinterpret_cast<const char *>(&noOverflowPointer), sizeof(noOverflowPointer));
        dataFile.write(reinterpret_cast<const char *>(&emptyRecordCount), sizeof(emptyRecordCount));

        // Account for the metadata space used in the total size calculation
        totalBlockSize += sizeof(noOverflowPointer) + sizeof(emptyRecordCount); // Renamed currentTotalSize to totalBlockSize

        // Increment the count of total blocks managed by the index
        totalBlocks++; // Keeping the naming consistent with the rest of the updated variables

        // Return the physical index of the newly initialized block
        return newBlockIndex;
    }


    // Writes a record to the physical index file, managing space within blocks and creating overflow blocks as necessary
    void storeRecordInDataFile(Record dataRecord, int startingBlockIndex, fstream &dataFileStream) {
        bool recordStored = false;

        while (!recordStored) {
            // Load the block at the starting index
            DataBlock currentDataBlock(startingBlockIndex);
            currentDataBlock.loadRecordFromStorage(dataFileStream);

            // Check if the record can fit in the current block, including handling overflow blocks
            if (currentDataBlock.utilizedSpace + dataRecord.calcSize() <= BLOCK_SIZE) {
                // Enough space available, proceed to store the record
                dataFileStream.seekp((startingBlockIndex * BLOCK_SIZE) + currentDataBlock.utilizedSpace);
                dataRecord.writeSerializeRecord(dataFileStream);

                // Update the number of records in the current block
                int updatedRecordCount = currentDataBlock.recordCount + 1;
                dataFileStream.seekp((startingBlockIndex * BLOCK_SIZE) + sizeof(int)); // Move past overflow pointer
                dataFileStream.write(reinterpret_cast<const char *>(&updatedRecordCount), sizeof(updatedRecordCount));

                // Adjust the total data size with the new record
                totalBlockSize += dataRecord.calcSize();

                recordStored = true;
            } else if (currentDataBlock.nextBlockIndex != -1) {
                // Current block is full, but there's an overflow block linked
                startingBlockIndex = currentDataBlock.nextBlockIndex;
            } else {
                // No space left in the current block and no overflow block exists, create a new overflow block
                int newOverflowIndex = createOverflowBucket(startingBlockIndex, dataFileStream);

                // Write the record to the newly created overflow block
                dataFileStream.seekp((newOverflowIndex * BLOCK_SIZE) + sizeof(int) * 2); // Skip to the record start position
                dataRecord.writeSerializeRecord(dataFileStream);

                // Update the overflow block's record count
                int initialRecordCount = 1;
                dataFileStream.seekp((newOverflowIndex * BLOCK_SIZE) + sizeof(int)); // Position at record count field
                dataFileStream.write(reinterpret_cast<const char *>(&initialRecordCount), sizeof(initialRecordCount));

                // Update the size tracker and mark the record as stored
                totalBlockSize += dataRecord.calcSize();
                recordStored = true;
            }
        }
    }


    // Insert new record into index
    void insertRecord(Record record, fstream &indexFile) {

        // No records written to index yet
        if (numRecords == 0) {
            // Initialize index with first blocks (start with 4)
            for (int j = 0; j < 2; j++) {
                initializeNewBucket(indexFile);
            }
            i = 1;

        }


        // Hash the record ID and get the last i bits to determine the bucket index
        int bucketIndex = getLastIthBits(hash(record.id), i);

        // Adjust bucket index if it's greater than or equal to the number of buckets
        if (bucketIndex >= n) {
            bucketIndex &= ~(1 << (i - 1));
        }

        // Get the physical page index from the blockDirectory using the bucket index
        int physicalPageIndex = blockDirectory[bucketIndex];

        // Write the record to the correct spot in the index file, creating overflow blocks if necessary
        storeRecordInDataFile(record, physicalPageIndex, indexFile);

        // Increment the record count
        numRecords++;

        // Check if rehashing is needed based on the average capacity per bucket exceeding 70%
        double avgCapacityPerBucket = static_cast<double>(totalBlockSize) / n;
        if (avgCapacityPerBucket > 0.7 * BLOCK_SIZE) {
            // Add a new bucket and rehash records as necessary
            int newBucketIndex = initializeNewBucket(indexFile);
            int digitsNeeded = static_cast<int>(ceil(log2(n + 1)));

            // Determine the real bucket index from which records should be moved
            int realBucketIndex = newBucketIndex & ~(1 << (digitsNeeded - 1));

            // Reallocate blocks for the bucket being split and move appropriate records
            int newPhysicalPageIndexForOldBucket = setupNewBlock(indexFile);
            int currentPageIndex = blockDirectory[realBucketIndex];

            while (currentPageIndex != -1) {
                DataBlock oldBlock(currentPageIndex);
                oldBlock.populateBlock(indexFile);


                indexFile.seekp(oldBlock.physicalIndex * BLOCK_SIZE);
                indexFile << string(BLOCK_SIZE, '*');

                totalBlocks--;
                numOverflowBlocks--;

                currentTotalSize -= oldBlock.blockSize;
                numRecords -= oldBlock.numRecords;

                for (int i = 0; i < oldBlock.numRecords; i++) {

                    // Consider last digitsToAddrNewBucket number of bits for each record
                    // If it matches the index of the new bucket then move to new bucket's block
                    // otherwise write to new old bucket block.
                    // Ex. For third new bucket at index 2 (binary: 10), we look at index 0 bucket for rehash and moving
                    // ghost keys; we now need to consider last 2 binary digits for each hashed id to see if it stays in current old
                    // bucket (last binary digits are 00) or gets moved to new bucket at index 2 (last binary digits are 10).
                    if (getLastIthBits(hash(oldBlock.records[i].id), digitsToAddrNewBucket) == newBucketIdx) {

                        // Put record in new bucket
                        int newBucketBlockPgIdx = pageDirectory[newBucketIdx];
                        writeRecordToIndexFile(oldBlock.records[i], newBucketBlockPgIdx, indexFile);

                    }
                    else {

                        // Put record in "new" old bucket that will have all ghost keys removed
                        int tempNewOldBlockPgIdx = newOldBucketPgIdx;
                        writeRecordToIndexFile(oldBlock.records[i], tempNewOldBlockPgIdx, indexFile);

                    }

                    numRecords++;

                }

                // Logic to read the block, clean it up, and decide which records need to be moved
                // would be similar to the described process, adjusting for the new variable names
                // and the conceptual shift from pageDirectory to blockDirectory, etc.

                // This would include reading each record, checking if it belongs in the new bucket or the old one
                // based on the updated hash, and then writing it to the appropriate block.

                currentPageIndex = -1; // Placeholder to simulate moving to the next block
            }

            numOverflowBlocks++;

            // Update the block directory to point to the new blocks for both the old and new buckets
            blockDirectory[realBucketIndex] = newPhysicalPageIndexForOldBucket;

            // Update the 'i' variable as necessary
            i = digitsNeeded;
        }


        // Add record to the index in the correct block, creating a overflow block if necessary


        // Take neccessary steps if capacity is reached:
		// increase n; increase i (if necessary); place records in the new bucket that may have been originally misplaced due to a bit flip


    }




public:
    LinearHashIndex(string indexFileName) {
        n = 4; // Start with 4 buckets in index
        i = 2; // Need 2 bits to address 4 buckets
        numRecords = 0;
        nextFreeBlock = 0;
        fName = indexFileName;

        // Create your EmployeeIndex file and write out the initial 4 buckets
        // make sure to account for the created buckets by incrementing nextFreeBlock appropriately
      
    }

    // Read csv file and add records to the index
    void createFromFile(string csvFName) {
        
    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {
        
    }
};
