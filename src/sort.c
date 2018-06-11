/*
** BSD 3-Clause License
**
** Copyright (c) 2018, David Boucher
** All rights reserved.
**
** Redistribution and use in source and binary forms, with or without
** modification, are permitted provided that the following conditions are met:
**
** * Redistributions of source code must retain the above copyright notice,
**   this list of conditions and the following disclaimer.
**
** * Redistributions in binary form must reproduce the above copyright notice,
**   this list of conditions and the following disclaimer in the documentation
**   and/or other materials provided with the distribution.
**
** * Neither the name of the copyright holder nor the names of its
**   contributors may be used to endorse or promote products derived from
**   this software without specific prior written permission.
**
** THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
** AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
** IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
** ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
** LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
** CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
** SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
** INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
** CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
** ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
** POSSIBILITY OF SUCH DAMAGE.
*/
#include <string.h>
#include "sort.h"
#include "tabular.h"

/**
 *  Le is a function returning 1 if array[i] <= array[j] following type
 *
 * @param array An array of RedisModuleStrings
 * @param type A char giving values types,
 *          * 'a' for strings ordered from the lesser to the greater
 *          * 'A' for strings ordered from the greater to the lesser
 *          * 'n' for numbers ordered from the lesser to the greater
 *          * 'N' for numbers ordered from the greater to the lesser
 * @param i * Index of the first element to compare
 * @param j * Index of the second element to compare
 * @param block_size
 *
 * @return 1 if array[i] <= array[j], 0 otherwise.
 */
static int Le(RedisModuleString **array, char *type, int i, int j, int block_size) {
    size_t len;
    char *t = type;
    RedisModuleString *tmp;
    for (int k = 0; k < block_size; ++k, ++t) {
        if (*t == 'a' || *t == 'A') {
            const char *ai = RedisModule_StringPtrLen(array[i + k], &len);
            const char *aj = RedisModule_StringPtrLen(array[j + k], &len);
            int cmp = strcmp(ai, aj);
            if (cmp) {
                if (*t == 'a')
                    return cmp < 0;
                else    /* 'A' */
                    return cmp > 0;
            }
        }
        else if (*t == 'n' || *t == 'N') {
            long long ai, aj;
            tmp = array[i + k];
            if (!tmp
                || RedisModule_StringToLongLong(tmp, &ai) == REDISMODULE_ERR)
                ai = 0;

            tmp = array[j + k];
            if (!tmp
                || RedisModule_StringToLongLong(tmp, &aj) == REDISMODULE_ERR)
                aj = 0;
            if (ai != aj) {
                if (*t == 'n')
                    return ai < aj;
                else    /* 'N' */
                    return ai > aj;
            }
        }
    }
    return 1;
}

/**
 *  Partition The function realizes the most important part of the quick sort
 *  algorithm.
 *
 * @param array The array to sort. Columns are flat, that is to say for an array
 *              containing two columns name and value, array is as follows
 *              array[0] = a name, array[1] = a value, array[2] = a name, etc...
 * @param type An array of types for each column of the array
 * @param block_size The group size in the array
 * @param begin The lower bound of the window wanted by the user
 * @param last The upper bound of the window wanted by the user
 *
 * @return The pivot index used by the algorithm
 */
static int Partition(RedisModuleString **array, char *type, int block_size,
                     int begin, int last) {
    int store_idx = begin;
    for (int i = begin; i < last; i += block_size) {
        if (Le(array, type, i, last, block_size)) {
            Swap(array, block_size, i, store_idx);
            store_idx += block_size;
        }
    }
    Swap(array, block_size, store_idx, last);
    return store_idx;
}

/**
 *  QuickSort The main function of the QuickSort algorithm. This
 *  implementation contains an optimization, so that the sort is not totally
 *  done on data ouside of the window scope.
 *
 * @param array The array to sort
 * @param type An array of the columns types.
 * @param begin The lower bound of elements to sort
 * @param last The upper bound of elements to sort
 * @param ldown The lower bound of the window wanted by the user
 * @param lup The upper bound of the window wanted by the user
 * @param block_size the group size in the array
 *
 * The order is total only from ldown to lup.
 */
void QuickSort(RedisModuleString **array, char *type, int block_size,
                      int begin, int last, int ldown, int lup) {
    int pivot_idx = 0;
    if (begin < last) {
        pivot_idx = Partition(array, type, block_size, begin, last);
        if (pivot_idx - block_size >= ldown) {
            QuickSort(array, type, block_size, begin, pivot_idx - block_size, ldown, lup);
        }
        if (pivot_idx + block_size <= lup) {
            QuickSort(array, type, block_size, pivot_idx + block_size, last, ldown, lup);
        }
    }
}

