#include <string.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <bitset>
#include <algorithm>

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
        cout << "ID of the candidate : " << id << "\n\n";
        cout << "Name of the candidate: " << name << "\n\n";
        cout << "Bio of the candidate : " << bio << "\n\n";
        cout << "Manager_id of the candidate: " << manager_id << "\n\n";
    }
};


class StorageBufferManager {
private:
    const int static BLOCK_SIZE = 4096;
    const int static MAX_PAGES = 3;
    string fileName;
    ofstream outFile;
    ifstream inFile;
    char page[MAX_PAGES * BLOCK_SIZE];
    int pageIndex = 0;
    int pageOffset = 0;
    int numRecords = 0;

    void addRecordToBuffer(Record record) {

        stringstream ss;
        ss.write(reinterpret_cast<const char*>(&record.id), sizeof(record.id));
        ss << record.name << '\0';
        ss << record.bio << '\0';
        ss.write(reinterpret_cast<const char*>(&record.manager_id), sizeof(record.manager_id));
        string record_str = ss.str();
        int record_size = record_str.size();

        if (pageOffset + record_size > BLOCK_SIZE) {
            outFile.write(page + pageIndex * BLOCK_SIZE, BLOCK_SIZE);
            pageIndex = (pageIndex + 1) % MAX_PAGES;
            pageOffset = 0;
        }

        std::copy(record_str.begin(), record_str.end(), page + pageIndex * BLOCK_SIZE + pageOffset);
        pageOffset += record_size;
        numRecords++;
    }

public:
    StorageBufferManager(string NewFileName) : fileName(NewFileName) {
        outFile.open(NewFileName, ios::binary);
    }

    void loadRecordsFromFile(string csvFName) {
        ifstream file(csvFName);
        string line;
        while (getline(file, line)) {
            stringstream ss(line);
            vector<string> fields;
            string field;


            while (getline(ss, field, ',')) {
                fields.push_back(field);
            }

            Record record(fields);
            addRecordToBuffer(record); // Changed from createFromFile to loadRecordsFromFile
        }
        if (pageOffset > 0) {
            outFile.write(page + pageIndex * BLOCK_SIZE, BLOCK_SIZE);
        }

        outFile.close();
    }

    Record retrieveRecordById(int id) {
        inFile.open(fileName, ios::binary);
        char page[BLOCK_SIZE * MAX_PAGES];
        int page_i = 0;

        while (inFile.read(page + page_i * BLOCK_SIZE, BLOCK_SIZE)) {
            int page_ofst = 0;


            while (page_ofst < BLOCK_SIZE) {
                int record_id;
                std::copy_n(page + page_i * BLOCK_SIZE + page_ofst, sizeof(int), reinterpret_cast<char*>(&record_id));

                if (record_id == id) {
                    vector<string> fields;
                    fields.push_back(to_string(record_id));
                    page_ofst += sizeof(int);

                    fields.push_back(string(page + page_i * BLOCK_SIZE + page_ofst));
                    page_ofst += fields.back().size() + 1;

                    fields.push_back(string(page + page_i * BLOCK_SIZE + page_ofst));
                    page_ofst += fields.back().size() + 1;

                    int manager_id;
                    std::copy_n(page + page_i * BLOCK_SIZE + page_ofst, sizeof(int), reinterpret_cast<char*>(&manager_id));
                    fields.push_back(to_string(manager_id));

                    Record record(fields);
                    inFile.close();
                    return record;
                } else {

                    page_ofst += sizeof(int);
                    page_ofst += strlen(page + page_i * BLOCK_SIZE + page_ofst) + 1;
                    page_ofst += strlen(page + page_i * BLOCK_SIZE + page_ofst) + 1;
                    page_ofst += sizeof(int);
                }
            }
            page_i = (page_i + 1) % MAX_PAGES;
        }

        inFile.close();
        cout<< "Record not found";
        exit(0);
        }
};