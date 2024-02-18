/*
Skeleton code for linear hash indexing
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
    while (true){
        cout << "Enter ID to search : ";
        int user_input;
        cin >> user_input;


        // Check for input failure
        if (cin.fail()) {
            cin.clear(); // Clear the error flag
            cin.ignore(numeric_limits<streamsize>::max(), '\n'); // Ignore the incorrect input
            cout << "Invalid input. Please enter a numeric value.\n";
            continue; // Skip the rest of the loop and prompt again
        }

        if (user_input > 0) {

            cout << "Searching for ID " << user_input << "\n";

            //Record targetRecord = emp_index.findRecordById(user_input);

            // Print record
            //targetRecord.print();

        }
        else
            break;

    }

    return 0;
}
