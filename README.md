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

### TABULAR.GET

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

Each column can be sorted alphabetically or numerically (also in reverse order), for that purpose we have keywords `ALPHA`, `NUM`, `REVALPHA` and `REVNUM`.

The `SORT` word needs also how many columns the sort works on, in the example, it works on 2 columns.

The result is an array containing the total count of rows (not just the window range) followed by the sorted rows contained in the window.

It is also possible to store this result in an ordered set. For that, we have the word `STORE` followed by a key name. And the full size is stored in
the key made of the store key name followed by `:size`.

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
> TABULAR.GET test 3 6 SORT 1 value NUM
1) (integer) 7
2) "s4"
3) "s3"
4) "s2"
5) "s1"
> TABULAR.GET test 3 6 SORT 1 value NUM STORE tmp
OK
> GET tmp:size
"7"
```

There is also a possibility to filter rows:
* with *wildcards* thanks to the `MATCH` keyword
* with strict equality thanks to the `EQUAL` keyword
* with field in a set thanks to the `IN` keyword

For example, with our example, we can keep only lines matching a pattern:
```
> tabular.get test 0 10 filter 1 descr MATCH "*6*"
1) "s6"
```

A query of the form `TABULAR.GET test 0 10 FILTER 1 descr EQUAL foo` keeps
only rows with `descr` containing exactly *foo*. The returned result contains
only rows from index 0 to 10, that is the 11 first rows.

And now, if we have a set `bag` containing several strings, it is possible to
filter rows having one field contained by `bag`. The syntax is the following:
```
> tabular.get test 0 10 filter 1 descr IN bag
1) "s6"
```

Operations are made in the following order:
1. FILTER
2. SORT
3. STORE

### TABULAR.COUNT

This function was needed to get counts of several data in the array.
The idea is to count rows but classified following one column, or even two, three columns.

For example, let's consider the following tabular:

| Status | Name   | location |
|:------:|:-------|:---------|
|0       |Mizar   |Tours     |
|0       |Altair  |Lyon      |
|1       |Arctarus|Agen      |
|3       |Vega    |Bordeaux  |
|4       |Vega    |Versailles|

We can represent those rows in Redis with the following lines:
```
> sadd rows r:1 r:2 r:3 r:4 r:5
(integer)5
>> hmset r:1 status 0 name Mizar location Tours
OK
> hmset r:2 status 0 name Altair location Lyon
OK
> hmset r:3 status 1 name Arctarus location Agen
OK
> hmset r:4 status 3 name Vega location Bordeaux
OK
> hmset r:5 status 4 name Vega location Versailles
OK
```

Now we want to count rows grouped by status, we can enter the following command:

```
> TABULAR.COUNT rows FILTER 1 status MATCH '[0-4]'
 1) "value"
 2) "1"
 3) "count"
 4) (integer) 1
 5) "children"
 6) (nil)
 7) "value"
 8) "0"
 9) "count"
10) (integer) 2
11) "children"
12) (nil)
13) "value"
14) "3"
15) "count"
16) (integer) 1
17) "children"
18) (nil)
19) "value"
20) "4"
21) "count"
22) (integer) 1
23) "children"
24) (nil)
```

So in this example, we apply a filter on `status` and we only take in count rows
matching this filter. The result is an array of three keys/values.

The first key is `value`, here *1*, the second key is `count`, here 1. That means there is 1 line with the status 1. The third key is `children`, here not filled and used only on multiple filters.

If we continue to look at the result, we get 2 rows with status 0, 1 row with status 3 and 1 row with status 4.

**Remark** - At the moment no limit are made on the filters, if you make a
query of the form
```
TABULAR.COUNT rows FILTER 1 name MATCH '*'
```

This can lead to very bad things because of the numerous cases. In a near future a limit will be added to avoid such non sense big result.

As we said previously, it is also possible to have filters on several columns:
```
> tabular.count rows filter 2 status MATCH '[0-4]' name EQUAL Vega
 1) "value"
 2) "1"
 3) "count"
 4) (integer) 1
 5) "children"
 6) (nil)
 7) "value"
 8) "0"
 9) "count"
10) (integer) 2
11) "children"
12) (nil)
13) "value"
14) "3"
15) "count"
16) (integer) 1
17) "children"
18) 1) "value"
    2) "Vega"
    3) "count"
    4) (integer) 1
    5) "children"
    6) (nil)
19) "value"
20) "4"
21) "count"
22) (integer) 1
23) "children"
24) 1) "value"
    2) "Vega"
    3) "count"
    4) (integer) 1
    5) "children"
    6) (nil)
```

Here, the result is given as a tree. We always have 1 row with status 1 and 2 rows with status 0.

Then for status 3, we get 1 row, but here the `children` key is filled with a new array of the same form, among the previous rows, we know that 1 contains the name 'Vega'.

And we have a similar result for status 4.

To end with this new function, we can use the keyword STORE:
```
> tabular.count rows filter 2 status MATCH '[0-4]' name EQUAL Vega STORE test
OK
> keys test:count:*
1) "test:count:4:Vega"
2) "test:count:0"
3) "test:count:1"
4) "test:count:3"
5) "test:count:3:V"
6) "test:count:4"
> mget test:count:4:Vega test:count:0 test:count:1 test:count:3 test:count:3:Vega test:count:4
1) "1"
2) "2"
3) "1"
4) "1"
5) "1"
6) "1"
```

Each result is stored in a key formed of the given name followed by `count` and followed by each found value.
