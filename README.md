# cBiK (Compact Bitmaps and Implicit KD-Tree)

Compact data structure to solve spatio-textual queries (Boolean kNN, Ranked kNN and Range Searching).
The main file has different instructions to make the calls both to build the index and to execute the queries found in the Build_cBiK file.

### The queries format to Range Searching:

**Latitude1 Longitude1 Latitude2 Longitude2 Keywords**

* 14.3956825620347 120.9119237743221 14.45925975974265 120.9775720752873 Imus Edwin Cavite

### Required library
* [sdsl-lite](https://github.com/simongog/sdsl-lite), instruction to install is included, need to run in Linux.

### How to run cbik
* In terminal, run make and then ./program.
* Before, compiling and running unzip the data.zip
* Uncomment or comment the push_back() functions (starting line 128) on vector queryFiles to run the queries that you desire.
* Change the name of dataSet on line 175, to use either data files in tweet2000000 or tweet4000000.
* Modify the for loop in line 174 to i <= 39 for tweet4000000 and i <= 19 for tweet2000000.
* We run all query files with tweet4000000 and only run datasize2000000.txt query file with tweet2000000.


