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
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
#include <cstdint>
#include <stdexcept>
#include <algorithm>

using namespace std;

class Record {
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields) {
        if (fields.size() != 4) {
            throw std::runtime_error("Invalid record format");
        }
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }
    Record(int invalidId) : id(invalidId), manager_id(-1), bio(""), name("") {}

    void print() {

            cout << "\tID: " << id << "\n";
            cout << "\tNAME: " << name << "\n";
            cout << "\tBIO: " << bio << "\n";
            cout << "\tMANAGER_ID: " << manager_id << "\n";
    }
};


class StorageBufferManager {
private:
    const int static BLOCK_SIZE = 4096;
    const int static MAXIMUM_BLOCKS_IN_MEMORY = 3;
    char RecordBuffer[MAXIMUM_BLOCKS_IN_MEMORY * BLOCK_SIZE];
    string DataFile;
    int bufferIndex = 0;
    int bufferCursor = 0;
    int totalRecords = 0;
    ofstream bufferWriter;
    ifstream bufferReader;

    void addRecordToBuffer(Record record) {

        stringstream recordStream;
        recordStream.write(reinterpret_cast<const char*>(&record.id), sizeof(record.id));
        recordStream << record.name << '\0';
        recordStream << record.bio << '\0';
        recordStream.write(reinterpret_cast<const char*>(&record.manager_id), sizeof(record.manager_id));
        string record_str = recordStream.str();
        int recordSize = record_str.size();

        // Check if the record size exceeds the block size
        if (recordSize > BLOCK_SIZE) {
            throw std::runtime_error("Record size exceeds the allowed block size.");
        }

        if (bufferCursor + recordSize > BLOCK_SIZE) {
            bufferWriter.write(RecordBuffer + bufferIndex * BLOCK_SIZE, BLOCK_SIZE);
            bufferIndex = (bufferIndex + 1) % MAXIMUM_BLOCKS_IN_MEMORY;
            bufferCursor = 0;
        }

        std::copy(record_str.begin(), record_str.end(), RecordBuffer + bufferIndex * BLOCK_SIZE + bufferCursor);
        bufferCursor += recordSize;
        totalRecords++;
    }

public:
    StorageBufferManager(string NewFileName) : DataFile(NewFileName) {
        bufferWriter.open(NewFileName, ios::binary);
    }

    void createFromFile(string csvFName) {
        ifstream csvFile(csvFName);
        // Validate if the file is open
        if (!csvFile.is_open()) {
            throw runtime_error("Failed to open CSV file: " + csvFName);
        }
        string line;
        while (getline(csvFile, line)) {
            stringstream lineStream(line);
            vector<string> record;
            string cell;

            while (getline(lineStream, cell, ',')) {
                record.push_back(cell);
            }

            Record empRecord(record);
            addRecordToBuffer(empRecord);
        }
        if (bufferCursor > 0) {
            bufferWriter.write(RecordBuffer + bufferIndex * BLOCK_SIZE, BLOCK_SIZE);
        }

        bufferWriter.close();
    }

    Record findRecordById(int id) {
        bufferReader.open(DataFile, ios::binary);
        char binaryTemp[BLOCK_SIZE * MAXIMUM_BLOCKS_IN_MEMORY];
        int tempIndex = 0;

        while (bufferReader.read(binaryTemp + tempIndex * BLOCK_SIZE, BLOCK_SIZE)) {
            int offset = 0;
            while (offset < BLOCK_SIZE) {
                int record_id;
                std::copy_n(binaryTemp + tempIndex * BLOCK_SIZE + offset, sizeof(int), reinterpret_cast<char*>(&record_id));

                if (record_id == id) {
                    vector<string> unpacked;
                    unpacked.push_back(to_string(record_id));
                    offset += sizeof(record_id);

                    for (int j = 0; j < 2; ++j) { // Name and Biography
                        string strField(binaryTemp + tempIndex * BLOCK_SIZE + offset);
                        offset += strField.size() + 1;
                        unpacked.push_back(strField);
                    }

                    int supervisorID;
                    std::copy_n(binaryTemp + tempIndex * BLOCK_SIZE + offset, sizeof(int), reinterpret_cast<char*>(&supervisorID));
                    unpacked.push_back(to_string(supervisorID));

                    Record foundEmp(unpacked);
                    bufferReader.close();
                    return foundEmp;

                } else {

                    offset += sizeof(int);
                    offset += strlen(binaryTemp + tempIndex * BLOCK_SIZE + offset) + 1;
                    offset += strlen(binaryTemp + tempIndex * BLOCK_SIZE + offset) + 1;
                    offset += sizeof(int);
                }
            }
            tempIndex = (tempIndex + 1) % MAXIMUM_BLOCKS_IN_MEMORY;
        }

        bufferReader.close();
        Record EmptyRecord(-1) ;
        return EmptyRecord;
        }
};