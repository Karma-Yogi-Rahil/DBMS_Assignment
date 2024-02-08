/*
Skeleton code for storage and buffer management


*/

#include <string>
#include <ios>
#include <fstream>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cmath>
#include "classes.h"
#include <cstdint>
using namespace std;


int main(int argc, char* const argv[]) {

    // Create the EmployeeRelation file from Employee.csv
    StorageBufferManager manager("EmployeeRelation");
    try {
        manager.createFromFile("Employee.csv");
        manager.finishWriting();
    } catch (const std::exception& e) {
        cerr << "Error occurred: " << e.what() << endl;
        return 1;
    }

//    // Loop to lookup IDs until user is ready to quit
//    //int id;
//    int64_t id;
//    while (true) {
//        cout << "Enter an Employee ID to search for (or -1 to quit): ";
//        if (!(cin >> id)) {
//            cin.clear(); // Clear the error flag
//            cin.ignore(numeric_limits<streamsize>::max(), '\n');
//            cout << "Invalid input. Please enter a valid Employee ID \n ";
//            continue;
//        }
//
//        if (id == -1) break;
//
//        try {
//            Record record = manager.findRecordById(id);
//            if (record.id != -1) {
//                record.print();
//            } else {
//                cout << "Record not found." << endl;
//            }
//        } catch (const std::exception& e) {
//            cerr << "Error during search: " << e.what() << endl;
//        }
//    }
//
//    cout << "Exiting the program." << endl;
//    return 0;


    string input;
    while (true) {
        cout << "Enter Employee IDs to search for (comma-separated, or -1 to quit): ";
        getline(cin, input);

        if (input == "-1") break;

        stringstream ss(input);
        string idStr;
        while (getline(ss, idStr, ',')) {
            int64_t id;
            stringstream convert(idStr);
            if (!(convert >> id)) {
                cout << "Invalid input: " << idStr << ". Please enter valid Employee IDs.\n";
                continue;
            }
            try {
                Record record = manager.findRecordById(id);
                if (record.id != -1) {
                    record.print();
                } else {
                    cout << "Record not found for ID: " << id << endl;
                }
            } catch (const std::exception& e) {
                cerr << "Error during search: " << e.what() << endl;
            }
        }
    }

    cout << "Exiting the program." << endl;
    return 0;

}
