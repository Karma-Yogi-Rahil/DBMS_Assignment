/*


DBMS - Assignment w6


1. Abhishek Manyam - 934476801 - manyama@oregonstate.edu
2. Rishitha Pokalkar - 934474549 - pokalkar@oregonstate.edu


*/

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include "classes.h"
#include <stdexcept>

using namespace std;

int main(int argc, char* const argv[]) {
    // Create the index
    LinearHashIndex emp_index("EmployeeIndex");
    emp_index.createFromFile("Employee.csv");





    // Loop to lookup IDs until the user is ready to quit

       int id;
    try{

    while (true) {
 
        cout << "Enter an Employee ID (or q to quit): ";
        cin >> id;

        if (id == 'q')
            break;

    Record record = emp_index.findRecordById(id);
     if (record.id != -1) {
            cout << "Record found:\n";
            record.print();
        } 


}
    }
catch(const std::exception& e) {

    std::cout << "id :" << id << "  " << e.what() << std::endl;
}

    return 0;

};

