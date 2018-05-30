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
#include <fnmatch.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "redismodule.h"

struct _Header {
    RedisModuleString *field;
    char type;
    const char *search;
};

typedef struct _Header Header;

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
 *  Swap Echanges two elements of array
 *
 * @param array The array to work on.
 * @param i The index of the first element
 * @param j The index of the second element
 */
static void Swap(RedisModuleString **array, int i, int j) {
    for (int k = 0; k < block_size; ++k) {
        RedisModuleString *tmp = array[i + k];
        array[i + k] = array[j + k];
        array[j + k] = tmp;
    }
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
            Swap(array, i, store_idx);
            store_idx += block_size;
        }
    }
    Swap(array, store_idx, last);
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
 *  Filter A multicolumn string filter function
 *
 * @param array The array to apply filter on.
 * @param size The array size
 * @param header Informations on each column, it contains the filter patterns
 * @param block_size The number of columns.
 *
 * @return The new size of the array. The real size of array is not modified
 *         to avoid reallocations, with this information, we know that the new
 *         array must be read in the range [0, return value).
 */
static int Filter(RedisModuleString **array, int size,
                   Header *header, int block_size) {
    int retval = size - block_size;
    int i, j;
    for (j = 0; j < block_size - 1; ++j) {
        if (header[j].search) {
            size_t len;
            const char *pattern = header[j].search;
            i = 0;
            while (i <= retval) {
                const char *txt = RedisModule_StringPtrLen(array[i + j], &len);
                if (fnmatch(pattern, txt, FNM_NOESCAPE | FNM_CASEFOLD | FNM_EXTMATCH)) {
                    for (int k = 0; k < block_size; ++k)
                        array[i + k] = array[retval + k];
                    retval -= block_size;
                }
                else
                    i += block_size;
            }
        }
    }
    return retval + block_size;
}

/**
 *  SwapHeaders A function to exchange columns in the header
 *
 * @param a a header
 * @param b another header
 */
static void SwapHeaders(Header *a, Header *b) {
    Header tmp;
    memcpy(&tmp, a, sizeof(Header));
    memcpy(a, b, sizeof(Header));
    memcpy(b, &tmp, sizeof(Header));
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
static Header *ParseArgv(RedisModuleString **argv, int argc, int *size,
                   RedisModuleString **key_store) {
    size_t len;
    int idx = 4;
    Header *retval = RedisModule_Alloc((argc - 4) / 2 * sizeof (Header));
    *size = 0;
    Header *tmp = retval;
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
            long long num;
            idx++;
            if (idx >= argc
                || RedisModule_StringToLongLong(argv[idx], &num) == REDISMODULE_ERR) {
                RedisModule_Free(retval);
                return NULL;
            }
            idx++;
            while (num > 0 && idx < argc) {
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
                tmp->search = a;
                idx++;
                num--;
            }
            if (num > 0) {
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
 *  An implementation of a sort function for the centreon web interface.
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
    long long key_count;
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
    Header *header = ParseArgv(argv, argc, &block_size, &key_store);
    if (!header) {
        return RedisModule_ReplyWithError(
                ctx,
                "Err: The syntax is TABULAR.GET key ldown lup {STORE key}? {SORT {field {ALPHA|NUM|REVALPHA|REVNUM}}*}? {FILTER {field 'expr'}*}?");
    }
    Header *lst;

    /* A block contains each column asked in the command line + the field
     * key */
    ++block_size;

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCARD", "s", set);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: Unable to get the set card");
    int size = RedisModule_CallReplyInteger(reply) * block_size;
    RedisModule_FreeCallReply(reply);

    RedisModuleString **array = NULL;
    char type[block_size];
    type[block_size - 1] = 'a';

    int ldown = first * block_size;
    int lup = last * block_size;

    /* The window is outside data. We force size to 0. */
    if (ldown >= size)
        size = 0;
    if (lup >= size)
        lup = size - block_size;
    if (lup < ldown)
        lup = ldown;

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

        for (lst = header, i = 0; i < block_size - 1; lst++, ++i) {
            type[i] = lst->type;


            for (j = 0; j < size; j += block_size) {
                size_t len;
                reply = RedisModule_Call(
                        ctx, "HGET", "ss",
                        array[j + block_size - 1], lst->field);
                const char *str = RedisModule_CallReplyStringPtr(reply, &len);
                array[j + i] = str ?RedisModule_CreateString(ctx, str, len) : NULL;
                RedisModule_FreeCallReply(reply);
            }
        }

        size = Filter(array, size, header, block_size);
    }

    QuickSort(
            array, type,
            0, size - block_size,
            ldown, lup);

    if (key_store == NULL) {
        if (size > 0) {
            int s = (lup - ldown) / block_size + 1;
            RedisModule_ReplyWithArray(ctx, s);
            for (size_t i = ldown; i <= lup; i += block_size)
                RedisModule_ReplyWithString(ctx, array[i + block_size - 1]);
        }
        else
            RedisModule_ReplyWithArray(ctx, 0);
    }
    else {
        reply = RedisModule_Call(ctx,
                "UNLINK", "s", key_store);
        RedisModule_FreeCallReply(reply);
        if (size > 0) {
            long long w = 0;
            for (size_t i = ldown; i <= lup; i += block_size, ++w) {
                reply = RedisModule_Call(ctx, "ZADD", "sls",
                        key_store, w, array[i + block_size - 1]);
                RedisModule_FreeCallReply(reply);
            }
        }
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    for (size_t i = 0; i < size; ++i) {
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
