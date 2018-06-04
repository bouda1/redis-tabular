# redis-tabular
A Redis Module implementing a multicolumn sort on hsets

## Installation
To build this module, you need the cmake tool. If you want to execute unit tests
you also need the rmtest tool.

### Build
To build this module, execute:
```bash
mkdir build
cd build
cmake ..
make
```

In the build directory, you will find a file redistabular.so that is a Redis
module. You just have to load it in the Redis server.

### Unit tests
To execute unit tests, first compile the project (see previous section).
Then, in the tests directory, you will find a file `tabular.py`, you can execute it with the command:
```bash
python tabular.py
```

If *rmtest* is correctly installed, tests should be OK.

## Usage
At the origin, this functionality was just a Lua script but performances were
poor on numerous rows. This module goal is to be able to sort Redis hsets by
several fields, with the possibility to have filters on several of them with a
window feature to just output a part of the result.

The typical usage is a web page with a big tabular to display. As you can scroll
along the tabular, we need a range of rows to select. You have the
possibility to sort the columns, to add filter on them. And the tabular shows
just a part of data (for example 30 rows).

Example: We have hsets for the form `s1 = { descr: Descr1, value: 7 }`, `s2 = { descr: Descr2, value: 6 }`, ..., `s7 = { descr: Descr7, value: 1 }`.

We also have a set containing all the keys to sort, let say `test = { s1, s2, s3, ..., s7 }`.

Now we want to sort those data following descr and in case of equality, following value. We just have to enter the command:
```
TABULAR.GET test 0 10 SORT 2 descr ALPHA value NUM
```

The 0 10 range is to tell we want rows from index 0 to index 10.

Each column can be sort alphabetically or numerically (also in reverse order), for that purpose we have keywords `ALPHA`, `NUM`, `REVALPHA` and `REVNUM`.

The `SORT` word needs also how many columns the sort works on, in the example, it works on 2 columns.

The result is an array containing the total count of rows followed by the content of the set reordered.

It is also possible to store this result in an ordered set. For that, we have the word `STORE` followed by a key name. And the full size of the set is stored in
the key made of the store key name followed by `:size`. This size is usually
greater than the ordered set one because it does not take in count the window
range.

For our previous example, we can then execute:
```
> TABULAR.GET test 0 10 SORT 1 value NUM
1) (integer) 7
2) "s7"
3) "s6"
4) "s5"
5) "s4"
6) "s3"
7) "s2"
8) "s1"
> tabular.get test 0 10 sort 1 value num store sorted
OK
> zrange sorted 0 10
1) "s7"
2) "s6"
3) "s5"
4) "s4"
5) "s3"
6) "s2"
7) "s1"
> GET sorted:size
"7"
```

There is also a possibility to filter rows with *wildcards*. For example, with our example, we can keep only lines matching a pattern:
```
> tabular.get test 0 10 filter 1 descr "*6*"
1) "s6"
```

It is possible to combine all of them. Operations are made in the following order:
1. FILTER
2. SORT
3. STORE
