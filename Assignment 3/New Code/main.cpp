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
using namespace std;


int main(int argc, char* const argv[]) {

    // Create the index
    LinearHashIndex emp_index("EmployeeIndex");
    emp_index.createFromFile("Employee.csv");

    
    // Loop to lookup IDs until user is ready to quit
    string input;
    try{

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
                    Record record = emp_index.findRecordById(id);
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
    }
    catch(const std::exception& e) {

        std::cout << "id :" << input << "  " << e.what() << std::endl;
    }

    return 0;
}
