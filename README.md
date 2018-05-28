# redis-tabular
A Redis Module implementing a multicolumn sort on hsets

## Installation
To build this module, you need the cmake tool. If you want to execute unit tests
you also need the rmtest tool.

### Build
To build this module, execute:
mkdir build
cd build
cmake ..
make

In the build directory, you will find a file redistabular.so that is a Redis
module. You just have to load it in the Redis server.

### Unit tests
To execute unit tests, first compile the project (see previous section).
Then, in the tests directory, you will find a file tabular.py, you can execute it with the command:
python tabular.py

If rmtest is correctly installed, tests should be OK.

## Usage
At the origin, this functionality was just a Lua script but performances were
poor on numerous rows. This module goal is to be able to sort Redis hsets by
several fields, with the possibility to have filters on several of them with a
window feature to just output a part of the result.

The typical usage is a web page with a big tabular to represent. You have the
possibility to sort the columns, to add filter on them. And the tabular shows
just a part of data (for example 30 rows).
