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
#include "tabular.h"

/**
 *  Swap Echanges two elements of array
 *
 * @param array The array to work on.
 * @param block_size Number of columns in the tabular
 * @param i The index of the first element
 * @param j The index of the second element
 */
void Swap(RedisModuleString **array, int block_size, int i, int j) {
    for (int k = 0; k < block_size; ++k) {
        RedisModuleString *tmp = array[i + k];
        array[i + k] = array[j + k];
        array[j + k] = tmp;
    }
}

/**
 *  SwapHeaders A function to exchange columns in the header
 *
 * @param a a header
 * @param b another header
 */
static void SwapHeaders(TabularHeader *a, TabularHeader *b) {
    TabularHeader tmp;
    memcpy(&tmp, a, sizeof(TabularHeader));
    memcpy(a, b, sizeof(TabularHeader));
    memcpy(b, &tmp, sizeof(TabularHeader));
}

/**
 *  ParseArgv A function to parse the argv array from the fourth element.
 *
 * @param argv The array to parse
 * @param argc The size of argv
 * @param[out] size The resulting block size
 * @param[out] key_store If the keyword 'STORE' is used, key_store will contain
 *                          the key where the result will be stored.
 * @param flag An union of flags to specify what category to parse
 *
 * @return The array header
 */
TabularHeader *ParseArgv(RedisModuleString **argv, int argc, int *size,
                         RedisModuleString **key_store, int flag) {
    size_t len;
    int idx = 0;
    TabularHeader *retval = RedisModule_Alloc(argc / 2 * sizeof (TabularHeader));
    *size = 0;
    TabularHeader *tmp = retval;
    while (idx < argc) {
        const char *a = RedisModule_StringPtrLen(argv[idx], &len);
        if ((flag & TABULAR_STORE) && strncasecmp(a, "STORE", len) == 0) {
            idx++;
            if (idx < argc) {
                if (key_store)
                    *key_store = argv[idx];
                idx++;
            }
            else {
                RedisModule_Free(retval);
                return NULL;
            }
        }
        else if ((flag & TABULAR_SORT) && strncasecmp(a, "SORT", len) == 0) {
            long long num;
            int row = 0;
            idx++;
            if (idx < argc) {
                if (RedisModule_StringToLongLong(argv[idx], &num) == REDISMODULE_ERR) {
                    RedisModule_Free(retval);
                    return NULL;
                }
            }
            else {
                RedisModule_Free(retval);
                return NULL;
            }
            idx++;
            while (row < num && idx < argc) {
                tmp = retval;
                int i = 0;
                while (i < *size && RedisModule_StringCompare(argv[idx], tmp->field)) {
                    i++;
                    ++tmp;
                }
                if (i == *size) {
                    (*size)++;
                    tmp->field = argv[idx];
                    tmp->tool = TABULAR_NONE;
                    tmp->search = NULL;
                }
                /* In the case of SORT coming after FILTER, the sort order can
                 * be perturbed by the filtered fields. Here we force the order
                 * to follow the one wanted in SORT */
                if (i > row) {
                    SwapHeaders(&retval[row], tmp);
                    tmp = &retval[row];
                }
                idx++;
                if (idx >= argc) {
                    RedisModule_Free(retval);
                    return NULL;
                }
                a = RedisModule_StringPtrLen(argv[idx], &len);
                if (strncasecmp(a, "ALPHA", len) == 0)
                    tmp->type = 'a';
                else if (strncasecmp(a, "REVALPHA", len) == 0)
                    tmp->type = 'A';
                else if (strncasecmp(a, "NUM", len) == 0)
                    tmp->type = 'n';
                else if (strncasecmp(a, "REVNUM", len) == 0)
                    tmp->type = 'N';
                else {
                    RedisModule_Free(retval);
                    return NULL;
                }
                idx++;
                row++;
            }
            if (row < num) {
                RedisModule_Free(retval);
                return NULL;
            }
        }
        else if ((flag & TABULAR_FILTER) && strncasecmp(a, "FILTER", len) == 0) {
            long long count;
            idx++;
            /* Let's get the filtered column's count */
            if (idx >= argc
                || RedisModule_StringToLongLong(argv[idx], &count) == REDISMODULE_ERR) {
                RedisModule_Free(retval);
                return NULL;
            }
            idx++;
            while (count > 0 && idx < argc) {
                tmp = retval;
                int i = 0;
                while (i < *size && RedisModule_StringCompare(argv[idx], tmp->field)) {
                    i++;
                    ++tmp;
                }
                if (i == *size) {
                    (*size)++;
                    tmp->field = argv[idx];
                    tmp->type = 0;
                }
                idx++;
                if (idx >= argc) {
                    RedisModule_Free(retval);
                    return NULL;
                }
                a = RedisModule_StringPtrLen(argv[idx], &len);
                if (strncasecmp(a, "MATCH", len) == 0)
                    tmp->tool = TABULAR_MATCH;
                else if (strncasecmp(a, "EQUAL", len) == 0)
                    tmp->tool = TABULAR_EQUAL;
                else if (strncasecmp(a, "IN", len) == 0)
                    tmp->tool = TABULAR_IN;
                else {
                    RedisModule_Free(retval);
                    return NULL;
                }
                idx++;
                if (idx >= argc) {
                    RedisModule_Free(retval);
                    return NULL;
                }
                a = RedisModule_StringPtrLen(argv[idx], &len);
                tmp->search = a;
                idx++;
                count--;
            }
            if (count > 0) {
                RedisModule_Free(retval);
                return NULL;
            }
        }
        else {
            RedisModule_Free(retval);
            return NULL;
        }
    }

    return retval;
}

