#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
#include <cstdint>
#include <stdexcept>
using namespace std;

class Record {
public:
    //int id, manager_id;
    int64_t id, manager_id;

    std::string bio, name;

    Record(vector<std::string> fields) {
        if (fields.size() != 4) throw runtime_error("Invalid record format");
        id = std::stoll(fields[0]);
        name = fields[1].substr(0, 200); // Truncate if longer than 200 bytes
        bio = fields[2].substr(0, 500); // Truncate if longer than 500 bytes
        manager_id = std::stoll(fields[3]);
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
    
    const int BLOCK_SIZE = 4096; // initialize the  block size allowed in main memory according to the question


    // You may declare variables based on your need 
    const int  MAXIMUM_BLOCKS_IN_MEMORY = 3;
    fstream FileStream;
    int CurrentBlockIndex;
    int CurrentRecordBufferIndex;
    vector<char> RecordBuffer;
    string DataFile;


    // Insert new record 
    void insertRecord(Record record) {

        // Serializing the record
        string serialized = to_string(record.id) + "," + record.name + "," + record.bio + "," + to_string(record.manager_id) + "\n";

        int recordSize = serialized.size();

        // No records written yet
        if (CurrentBlockIndex + recordSize > BLOCK_SIZE) {
            FileStream.write(RecordBuffer.data(), CurrentBlockIndex);
            CurrentBlockIndex = 0;
        }

        // Add record to the block
        copy(serialized.begin(), serialized.end(), RecordBuffer.begin() + CurrentBlockIndex);
        CurrentBlockIndex += recordSize;


    }

public:
    StorageBufferManager(string NewFileName) {
        
        //initialize your variables
        DataFile = NewFileName;
        CurrentBlockIndex = 0;
        RecordBuffer.resize(BLOCK_SIZE); // Initialize single buffer

        if (FileStream.is_open()) {
            FileStream.close();
        }

        // Create your EmployeeRelation file
        FileStream.open(DataFile, ios::out | ios::binary | ios::trunc);
        if (!FileStream) {
            throw runtime_error("Cannot open file: " + DataFile);
        }
    }

    // Read csv file (Employee.csv) and add records to the (EmployeeRelation)
    void createFromFile(string csvFName) {
        ifstream csvFile(csvFName);
        if (!csvFile) {
            throw runtime_error("Cannot open file: " + csvFName);
        }

        if (csvFile.peek() == ifstream::traits_type::eof()) {
            cout << "Input CSV file is empty." << endl;
            return;
        }

        string line;
        while (getline(csvFile, line)) {
            size_t pos = 0;
            vector<string> fields;

            while ((pos = line.find(',')) != string::npos) {
                string field = line.substr(0, pos);
                fields.push_back(field);
                line.erase(0, pos + 1);
            }
            fields.push_back(line); // Add the last field

            Record record(fields);
            insertRecord(record);
        }

        // Write any remaining records in the buffer to the file
        if (CurrentBlockIndex > 0) {
            FileStream.write(RecordBuffer.data(), CurrentBlockIndex);
        }

        csvFile.close();
    }

    // Given an ID, find the relevant record and print it
  /*  Record findRecordById(int id) {
        // Set file stream pointer to the beginning of the file
        FileStream.seekg(0, ios::beg);

        vector<char> block(BLOCK_SIZE);
        string buffer;  // Buffer to accumulate partial records

        while (!FileStream.eof()) {
            // Clear the previous block's data
            std::fill(block.begin(), block.end(), 0);

            // Read a block of data
            FileStream.read(block.data(), BLOCK_SIZE);
            buffer += string(block.begin(), block.end());

            size_t pos = 0;
            while ((pos = buffer.find('\n')) != string::npos) {
                string line = buffer.substr(0, pos);
                buffer.erase(0, pos + 1);

                stringstream ss(line);
                vector<string> fields;
                string field;

                while (getline(ss, field, ',')) {
                    fields.push_back(field);
                }

                if (!fields.empty()) {
                    Record record(fields);
                    if (record.id == id) {
                        return record;  // Record found
                    }
                }
            }
        }
        return Record(vector<string>()); // Not found
    }
*/


  Record findRecordById(int id) {
        FileStream.clear(); // Clear any error flags
      if (!FileStream.is_open()) {
          // FileStream is not open, so open it here
          FileStream.open(DataFile, ios::in | ios::out | ios::binary);
          if (!FileStream) {
              throw runtime_error("Failed to open file: " + DataFile);
          }
      }
        FileStream.seekg(0, ios::beg); // Move to the start of the file

        vector<char> block(BLOCK_SIZE);
        string buffer;  // Buffer to accumulate partial records

        while (true) {
            FileStream.read(block.data(), BLOCK_SIZE);
            size_t bytesRead = FileStream.gcount();

            // If no bytes were read, break out of the loop
            if (bytesRead == 0) {
                break;
            }

            buffer.append(block.begin(), block.begin() + bytesRead);

            size_t pos;
            while ((pos = buffer.find('\n')) != string::npos) {
                string line = buffer.substr(0, pos);
                buffer.erase(0, pos + 1);

                stringstream ss(line);
                vector<string> fields;
                string field;

                while (getline(ss, field, ',')) {
                    fields.push_back(field);
                }

                if (!fields.empty()) {
                    Record record(fields);
                    if (record.id == id) {
                        return record;  // Record found
                    }
                }
            }
        }
        return { -1}; // Not found
    }

    void finishWriting() {

      // Write any remaining records in the buffer to the file
        if (CurrentBlockIndex > 0) {
            FileStream.write(RecordBuffer.data(), CurrentBlockIndex);
            CurrentBlockIndex = 0;
        }

        // Explicitly close the FileStream
        FileStream.close();
    }

};
