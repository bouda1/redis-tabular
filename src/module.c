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
#include <stdlib.h>
#include <string.h>
#include "count.h"
#include "filter.h"
#include "sort.h"

static RedisModuleString **GetArray(RedisModuleCtx *ctx, int size, int block_size, TabularHeader *header, RedisModuleString *set) {
    size_t i, j;
    RedisModuleString **array = RedisModule_Alloc(size * sizeof(RedisModuleString *));

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SMEMBERS", "s", set);
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
        if (key) {
            TabularHeader *lst;
            for (lst = header, i = 0; i < block_size - 1; lst++, ++i) {
                RedisModuleString *value;
                RedisModule_HashGet(key, REDISMODULE_HASH_NONE, lst->field, &value, NULL);
                array[j + i] = value;
            }
            RedisModule_CloseKey(key);
        }
        else
            for (i = 0; i < block_size - 1; ++i)
                array[j + i] = NULL;
    }
    return array;
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
    int block_size = 0;

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
    TabularHeader *header = ParseArgv(argv + 4, argc - 4, &block_size, &key_store,
            TABULAR_SORT | TABULAR_STORE | TABULAR_FILTER);
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
                "Err: The syntax is TABULAR.GET key ldown lup {STORE key}? {SORT {field {ALPHA|NUM|REVALPHA|REVNUM}}*}? {FILTER {field {MATCH|EQUAL|IN} 'expr'}*}?");
    }

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
    TabularHeader *lst;
    int i;
    int should_sort = 0;
    for (lst = header, i = 0; i < block_size - 1; lst++, i++) {
        type[i] = lst->type;
        if (type[i])
            should_sort = 1;
    }

    int ldown = first * block_size;

    /* The window is outside data. We force size to 0. */
    /* We already have to compute size because of its need for the filter. */
    if (ldown >= size)
        size = 0;

    if (size > 0) {
        array = GetArray(ctx, size, block_size, header, set);
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

    if (should_sort) {
        QuickSort(
                array, type, block_size,
                0, size - block_size,
                ldown, lup);
    }

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

static int TabularFilter_RedisCommand(RedisModuleCtx *ctx,
                                      RedisModuleString **argv,
                                      int argc) {
    int block_size = 0;

    if (argc < 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleString *set = argv[1];

    RedisModuleString *key_store = NULL;
    TabularHeader *header = ParseArgv(argv + 2, argc - 2, &block_size, &key_store,
            TABULAR_STORE | TABULAR_FILTER);

    if (!header) {
        return RedisModule_ReplyWithError(
                ctx,
                "Err: The syntax is TABULAR.FILTER key {STORE key}? {FILTER {field {MATCH|EQUAL|IN} 'expr'}*}?");
    }

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCARD", "s", set);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: Unable to get the set card");
    ++block_size;
    int size = RedisModule_CallReplyInteger(reply) * block_size;
    RedisModule_FreeCallReply(reply);

    RedisModuleString **array = GetArray(ctx, size, block_size, header, set);

    int orig_size = size;
    if (size > 0)
        size = Filter(ctx, array, size, header, block_size);

    if (key_store == NULL) {
        if (size > 0) {
            RedisModule_ReplyWithArray(ctx, size / block_size);
            for (size_t i = 0; i < size; i += block_size)
                RedisModule_ReplyWithString(ctx, array[i + block_size - 1]);
        }
        else
            RedisModule_ReplyWithArray(ctx, 0);
    }
    else {
        reply = RedisModule_Call(ctx, "UNLINK", "s", key_store);
        RedisModule_FreeCallReply(reply);
        if (size > 0) {
            int bs = block_size * 16;
            size_t i = 0;
            for (i = 0; i + bs <= size; i += bs) {
                reply = RedisModule_Call(ctx, "SADD", "sssssssssssssssss", key_store,
                                         array[i + block_size - 1],
                                         array[i + 2 * block_size - 1],
                                         array[i + 3 * block_size - 1],
                                         array[i + 4 * block_size - 1],
                                         array[i + 5 * block_size - 1],
                                         array[i + 6 * block_size - 1],
                                         array[i + 7 * block_size - 1],
                                         array[i + 8 * block_size - 1],
                                         array[i + 9 * block_size - 1],
                                         array[i + 10 * block_size - 1],
                                         array[i + 11 * block_size - 1],
                                         array[i + 12 * block_size - 1],
                                         array[i + 13 * block_size - 1],
                                         array[i + 14 * block_size - 1],
                                         array[i + 15 * block_size - 1],
                                         array[i + 16 * block_size - 1]);
                RedisModule_FreeCallReply(reply);
            }
            for (; i < size; i += block_size) {
                reply = RedisModule_Call(ctx, "SADD", "ss", key_store,
                                         array[i + block_size - 1]);
                RedisModule_FreeCallReply(reply);
            }
        }
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    for (size_t i = 0; i < orig_size; ++i) {
        if (array[i])
            RedisModule_FreeString(ctx, array[i]);
    }
    RedisModule_Free(array);
    RedisModule_Free(header);
    return REDISMODULE_OK;
}

static int TabularCount_RedisCommand(RedisModuleCtx *ctx,
                                     RedisModuleString **argv,
                                     int argc) {
    int block_size = 0;

    if (argc < 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleString *set = argv[1];

    RedisModuleString *key_store = NULL;
    TabularHeader *header = ParseArgv(argv + 2, argc - 2, &block_size, &key_store,
            TABULAR_STORE | TABULAR_FILTER);

    if (!header) {
        return RedisModule_ReplyWithError(
                ctx,
                "Err: The syntax is TABULAR.COUNT key {STORE key}? {FILTER {field {MATCH|EQUAL|IN} 'expr'}*}?");
    }

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCARD", "s", set);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: Unable to get the set card");
    ++block_size;
    int size = RedisModule_CallReplyInteger(reply) * block_size;
    RedisModule_FreeCallReply(reply);

    RedisModuleString **array = GetArray(ctx, size, block_size, header, set);

    CountList *cnt = Count(ctx, array, size, header, block_size);

    if (key_store == NULL)
        CountReply(ctx, cnt);
    else
        CountReplyStore(ctx, cnt, key_store);

    for (size_t i = 0; i < size; ++i) {
        if (array[i])
            RedisModule_FreeString(ctx, array[i]);
    }
    RedisModule_Free(array);
    RedisModule_Free(header);
    FreeCountList(cnt);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "tabular", 1, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "tabular.get",
        TabularGet_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "tabular.filter",
        TabularFilter_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "tabular.count",
        TabularCount_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
