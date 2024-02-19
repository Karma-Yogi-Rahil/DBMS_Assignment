#include <string>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <bitset>
#include <fstream>
#include<cmath>
#include <cstring>
#include <stdexcept>

using namespace std;

class Record {
public:
    int id, manager_id;
    char bio[500];  
    char name[100]; 


//default constructor, when im creating an object I'm setting the values to 0
    Record() : id(0), manager_id(0) {
        memset(name, 0, sizeof(name));
        memset(bio, 0, sizeof(bio));
    }

    //https://www.geeksforgeeks.org/memset-c-example/

    Record(vector<std::string>& fields) {
        id = stoi(fields[0]);
        strncpy(name, fields[1].c_str(), sizeof(name));
        name[sizeof(name) - 1] = '\0'; // Ensure null termination
        strncpy(bio, fields[2].c_str(), sizeof(bio));
        bio[sizeof(bio) - 1] = '\0'; // Ensure null termination
        manager_id = stoi(fields[3]);
    }

    void print() {
        cout << "ID: " << id << endl;
        cout << "NAME: " << name << endl;
        cout << "BIO: " << bio << endl;
        cout << "MANAGER_ID: " << manager_id << endl;

    }
};

class LinearHashIndex {
private:
    const int BLOCK_SIZE = 4096;
    const int MAX_BLOCKS_IN_MEMORY = 3;

    vector<int> blockDirectory;
    int n;
    int i;
    int nextFreeBlock;
    string fName;
    vector<int> initialBlockDirectory = blockDirectory;
 
    void insertRecord(Record record, fstream& indexFile) {

        //we are calculating the hash value and offset value so that we can 
        //put out data at a required destination

        int hashValue = record.id % static_cast<int>(pow(2, i));


        int offset = blockDirectory[hashValue] * BLOCK_SIZE;

        // using seekp to go to the location and then store the information

        indexFile.seekp(offset);
        
        /*
        
        if(record.id==11432142)
        {
            cout<<" "<<hashValue<<"   "<<offset<<"  ";
        }
        */

      
        indexFile.write(reinterpret_cast<const char*>(&record), sizeof(Record));

        blockDirectory[hashValue]++;
    }

public:
    LinearHashIndex(string indexFileName) {
        n = 4; 
        i = 2; 
        nextFreeBlock = 0;
        fName = indexFileName;

        // Create the index file and write out the initial 4 buckets
        fstream indexFile(fName, ios::binary | ios::out);
        indexFile.close();

        for (int j = 0; j < n; j++) {
            blockDirectory.push_back(j);
        }

        nextFreeBlock += n * BLOCK_SIZE;
    }

    // Read CSV file and add records to the index
    void createFromFile(string csvFName) {
        ifstream csvFile(csvFName);
 // we are opening the csv file 
        if (!csvFile.is_open()) {
            cout << "Error opening the CSV file.\n";
            return;
        }

        fstream indexFile(fName, ios::binary | ios::app | ios::out);

        string line;
        vector<Record> recordsInMemory;
        int blocksInMemory = 1;

        while (getline(csvFile, line)) {
            vector<string> fields;
            stringstream ss(line);
            string field;

            while (getline(ss, field, ',')) {
                fields.push_back(field);
            }

            if (fields.size() == 4) {
                Record record(fields);


// this is the most important step, we are considering the recordsInmemory variable to keep track of the memory we have
                recordsInMemory.push_back(record);
                
                // This is the code to check if we have reached max blocks
                //we are reading each record at a time, which make sures we are using less than 4096 every time, and this executes only 3 times.
                //after 3 times the max_blocks_in_memory will reset and the data is written to the file.
                if (blocksInMemory == MAX_BLOCKS_IN_MEMORY) {
                    // Write the records to the index file
                    for (const auto& rec : recordsInMemory) {
                        insertRecord(rec, indexFile);
                    }

                   
                    recordsInMemory.clear();
                    blocksInMemory = 0;

                }

                blocksInMemory++;
               
            }
        }

        // Write any remaining records to the index file
        for (const auto& rec : recordsInMemory) {
            insertRecord(rec, indexFile);
        }

        csvFile.close();
        indexFile.close();
    }

  


Record findRecordById1(int id) {
    Record record;
    record.id = -1; // Set id to -1 to indicate not found
    
    ifstream indexFile(fName, ios::binary | ios::in);
    
    if (indexFile.is_open()) {
        int blocksRead = 0; // Keep track of the number of blocks read
        int maxBlocksInMemory = min(MAX_BLOCKS_IN_MEMORY, n); // Limit the number of blocks to read
        
      
            vector<Record> recordsInMemory; // Buffer to store the new data
            int blocksInMemory = 1; // Number of blocks currently in memory
            
            while (indexFile.read(reinterpret_cast<char*>(&record), sizeof(Record))) {
                if (record.id == id) {
                    indexFile.close();
                    return record;
                }
                
                recordsInMemory.push_back(record);
                blocksInMemory++;
                
                // Check if the maximum number of blocks in memory is reached
                if (blocksInMemory == maxBlocksInMemory) {
                    // Clear the buffer and reload data
                    recordsInMemory.clear();
                    blocksInMemory = 1;
                }
            }
        

        indexFile.close();
    }
if (record.id == id) {
                    return record;
                }
                else {
throw runtime_error("Record not found");
                }
                
}


Record findRecordById(int id) {
    Record record;
    record.id = -1; // Set id to -1 to indicate not found

    fstream indexFile(fName, ios::binary | ios::in);

// If the file opened correctly
if (indexFile.is_open()) {
    // Calculate the block index using hash
    int blockIndex = id % static_cast<int>(pow(2, i));
    int currentBlock = 0;
    int blocksRead = 0; 
        int maxBlocksInMemory = MAX_BLOCKS_IN_MEMORY; // this will limit the number of blocks to read
        
      
            vector<Record> recordsInMemory; // creating a vector to act as a buffer to store the new data
            int Memoryinuse = 1; // Number of blocks currently in memory

// this snippet is used to skip till be go to the correct block
    while (currentBlock < blockIndex-1) {
        indexFile.read(reinterpret_cast<char*>(&record), sizeof(Record));
        
        currentBlock++;
    }

    // Now, search for the record in the block

        // Read the records from the block one by one
        //this will make sure I only use one block in the memory and when the blocks become 3 the if condition in the 250 line fails.

        while (indexFile.read(reinterpret_cast<char*>(&record), sizeof(Record))) {
            // If the record is the one we are looking for, return it
            //https://www.cs.uah.edu/~rcoleman/Common/CodeVault/Code/Code607_FileIO.html
            if (record.id == id) {
                indexFile.close();
                return record;
            }
        }
        recordsInMemory.push_back(record);
                Memoryinuse++;
                
                // Check if the maximum number of blocks in memory is reached
                if (Memoryinuse == maxBlocksInMemory) {
                    // Clear the buffer and reload data
                    recordsInMemory.clear();
                    Memoryinuse = 1;
                }
    
    indexFile.close();
}


throw runtime_error("Record not found");

// if no record is found, 
}

};



