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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tabular.h"
#include "filter.h"

static int block_size = 0;

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
 *
 * @return 1 if array[i] <= array[j], 0 otherwise.
 */
static int Le(RedisModuleString **array, char *type, int i, int j) {
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
 * @param begin The lower bound of the window wanted by the user
 * @param last The upper bound of the window wanted by the user
 *
 * @return The pivot index used by the algorithm
 */
static int Partition(RedisModuleString **array, char *type,
                     int begin, int last) {
    int store_idx = begin;
    for (int i = begin; i < last; i += block_size) {
        if (Le(array, type, i, last)) {
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
 *
 * The order is total only from ldown to lup.
 */
static void QuickSort(RedisModuleString **array, char *type,
                       int begin, int last, int ldown, int lup) {
    int pivot_idx = 0;
    if (begin < last) {
        pivot_idx = Partition(array, type, begin, last);
        if (pivot_idx - block_size >= ldown) {
            QuickSort(array, type, begin, pivot_idx - block_size, ldown, lup);
        }
        if (pivot_idx + block_size <= lup) {
            QuickSort(array, type, pivot_idx + block_size, last, ldown, lup);
        }
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
 *
 * @return The array header
 */
static TabularHeader *ParseArgv(RedisModuleString **argv, int argc, int *size,
                   RedisModuleString **key_store) {
    size_t len;
    int idx = 0;
    TabularHeader *retval = RedisModule_Alloc(argc / 2 * sizeof (TabularHeader));
    *size = 0;
    TabularHeader *tmp = retval;
    while (idx < argc) {
        const char *a = RedisModule_StringPtrLen(argv[idx], &len);
        if (strncasecmp(a, "STORE", len) == 0) {
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
        else if (strncasecmp(a, "SORT", len) == 0) {
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
        else if (strncasecmp(a, "FILTER", len) == 0) {
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

/**
 *  An implementation of a sort function
 *  The first argument is a Redis set to sort
 *  Following arguments are fields to sort, each one followed by ASC or DESC.
 *
 * @param ctx The Redis context
 * @param argv An array of arguments
 * @param argc The arguments count with the command. That is to say
 *             "tabular.get 2" gives argc=2
 *
 * @return REDISMODULE_ERR or REDISMODULE_OK
 */
static int TabularGet_RedisCommand(RedisModuleCtx *ctx,
                                   RedisModuleString **argv,
                                   int argc) {
    long long key_count = 0;
    long long first, last;

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *set = argv[1];
    if (RedisModule_StringToLongLong(argv[2], &first) == REDISMODULE_ERR)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: The second argument must be an integer");
    if (RedisModule_StringToLongLong(argv[3], &last) == REDISMODULE_ERR)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: The third argument must be an integer");
    if (first > last) {
        long long tmp = first;
        first = last;
        last = tmp;
    }

    RedisModuleString *key_store = NULL;
    TabularHeader *header = ParseArgv(argv + 4, argc - 4, &block_size, &key_store);
    RedisModuleString *keystore_size_str = NULL;
    if (key_store) {
        size_t len;
        const char *ptr = RedisModule_StringPtrLen(key_store, &len);
        keystore_size_str = RedisModule_CreateStringPrintf(
                ctx, "%s:size", ptr);
    }
    if (!header) {
        return RedisModule_ReplyWithError(
                ctx,
                "Err: The syntax is TABULAR.GET key ldown lup {STORE key}? {SORT {field {ALPHA|NUM|REVALPHA|REVNUM}}*}? {FILTER {field 'expr'}*}?");
    }
    TabularHeader *lst;

    /* A block contains each column asked in the command line + the field
     * key */
    ++block_size;

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCARD", "s", set);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: Unable to get the set card");
    int size = RedisModule_CallReplyInteger(reply) * block_size;
    int orig_size = 0;
    RedisModule_FreeCallReply(reply);

    RedisModuleString **array = NULL;
    char type[block_size];
    type[block_size - 1] = 'a';

    int ldown = first * block_size;

    /* The window is outside data. We force size to 0. */
    /* We already have to compute size because of its need for the filter. */
    if (ldown >= size)
        size = 0;

    if (size > 0) {
        array = RedisModule_Alloc(size * sizeof(char *));

        size_t i, j;

        reply = RedisModule_Call(ctx, "SMEMBERS", "s", set);
        for (i = block_size - 1, j = 0; i < size; i += block_size, ++j) {
            size_t len;
            const char *str = RedisModule_CallReplyStringPtr(
                    RedisModule_CallReplyArrayElement(reply, j), &len);
            array[i] = RedisModule_CreateString(ctx, str, len);
        }
        RedisModule_FreeCallReply(reply);

        for (j = 0; j < size; j += block_size) {
            RedisModuleKey *key = RedisModule_OpenKey(
                    ctx, array[j + block_size - 1], REDISMODULE_READ);
            for (lst = header, i = 0; i < block_size - 1; lst++, ++i) {
                type[i] = lst->type;
                size_t len;
                RedisModuleString *value;
                RedisModule_HashGet(key, REDISMODULE_HASH_NONE, lst->field, &value, NULL);
                array[j + i] = value;
            }
            RedisModule_CloseKey(key);
        }
        orig_size = size;
        size = Filter(ctx, array, size, header, block_size);
    }

    key_count = size / block_size;

    /* The window is outside data. We force size to 0.
     * After the filter, size may have changed */
    if (ldown >= size)
        size = 0;
    int lup = last * block_size;
    if (lup >= size)
        lup = size - block_size;
    if (lup < ldown)
        lup = ldown;

    QuickSort(
            array, type,
            0, size - block_size,
            ldown, lup);

    if (key_store == NULL) {
        if (size > 0) {
            int s = (lup - ldown) / block_size + 2;
            RedisModule_ReplyWithArray(ctx, s);
            RedisModule_ReplyWithLongLong(ctx, key_count);
            for (size_t i = ldown; i <= lup; i += block_size)
                RedisModule_ReplyWithString(ctx, array[i + block_size - 1]);
        }
        else {
            RedisModule_ReplyWithArray(ctx, 1);
            RedisModule_ReplyWithLongLong(ctx, key_count);
        }
    }
    else {
        RedisModuleKey *key = RedisModule_OpenKey(
                ctx, key_store, REDISMODULE_WRITE);
        RedisModule_DeleteKey(key);
        if (size > 0) {
            double w = 0;
            for (size_t i = ldown; i <= lup; i += block_size, ++w) {
                RedisModule_ZsetAdd(key, w, array[i + block_size - 1], NULL);
            }
            RedisModule_CloseKey(key);
        }
        reply = RedisModule_Call(ctx, "SET", "sl",
                keystore_size_str, key_count);
        RedisModule_FreeCallReply(reply);
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_FreeString(ctx, keystore_size_str);
    }

    for (size_t i = 0; i < orig_size; ++i) {
        if (array[i])
            RedisModule_FreeString(ctx, array[i]);
    }
    RedisModule_Free(array);
    RedisModule_Free(header);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "tabular", 1, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "tabular.get",
        TabularGet_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
