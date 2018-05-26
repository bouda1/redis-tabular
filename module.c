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

#include "redismodule.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int block_size = 0;

static int le(RedisModuleString **array, char *type, int i, int j) {
    size_t len;
    char *t = type;
    for (int k = 0; k < block_size; ++k) {
        if (*t == 'a' || *t == 'A') {
            const char *ai = RedisModule_StringPtrLen(array[i + k], &len);
            const char *aj = RedisModule_StringPtrLen(array[j + k], &len);
            int cmp = strcmp(ai, aj);
            if (cmp) {
                if (*t == 'a')
                    return cmp < 0 ? 1 : 0;
                else    /* 'A' */
                    return cmp > 0 ? 1 : 0;
            }
        }
        else {  /* The onyl choice here is 'n' or 'N' */
            long long ai, aj;
            if (RedisModule_StringToLongLong(array[i + k], &ai)
                    == REDISMODULE_ERR)
                ai = 0;
            if (RedisModule_StringToLongLong(array[j + k], &aj)
                    == REDISMODULE_ERR)
                aj = 0;
            if (*t == 'n')
                return ai < aj;
            else    /* 'N' */
                return ai > aj;
        }
    }
    return 1;
}

static void swap(RedisModuleString **array, int i, int j) {
    for (int k = 0; k < block_size; ++k) {
        RedisModuleString *tmp = array[i + k];
        array[i + k] = array[j + k];
        array[j + k] = tmp;
    }
}

static int partition(RedisModuleString **array, char *type,
                     int begin, int last) {
    int store_idx = begin;
    for (int i = begin; i < last; i += block_size) {
        if (le(array, type, i, last)) {
            swap(array, i, store_idx);
            store_idx += block_size;
        }
    }
    swap(array, store_idx, last);
    return store_idx;
}

static void quick_sort(RedisModuleString **array, char *type,
                       int begin, int last, int ldown, int lup) {
    int pivot_idx = 0;
    if (begin < last) {
        pivot_idx = partition(array, type, begin, last);
        if (pivot_idx - block_size >= ldown) {
            quick_sort(array, type, begin, pivot_idx - block_size, ldown, lup);
        }
        if (pivot_idx + block_size <= lup) {
            quick_sort(array, type, pivot_idx + block_size, last, ldown, lup);
        }
    }
}

/**
 *  An implementation of a sort function for the centreon web interface.
 *  The first argument is a Redis set to sort
 *  Following arguments are fields to sort, each one followed by ASC or DESC.
 *
 * @param ctx The Redis context
 * @param argv An array of arguments
 * @param argc The arguments count with the command. That is to say
 *             "tabular.sort 2" gives argc=2
 *
 * @return REDISMODULE_ERR or REDISMODULE_OK
 */
int TabularSort_RedisCommand(RedisModuleCtx *ctx,
                             RedisModuleString **argv,
                             int argc) {
    long long key_count;
    long long first, last;

    if (argc <= 5) {
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

    /* A block contains each column asked in the command line + the field
     * name */
    block_size = argc - 4;
    if (block_size % 2)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: For each column you must tell how to sort with (REV)ALPHA or (REV)NUM");
    /* We add one to store the field key at the last position of each block */
    block_size = block_size / 2 + 1;

    char type[block_size];
    type[block_size - 1] = 'a';
    for (size_t i = 0; i < block_size - 1; ++i) {
        size_t len;
        const char *a = RedisModule_StringPtrLen(argv[5 + 2 * i], &len);
        if (strncasecmp(a, "ALPHA", len) == 0)
            type[i] = 'a';
        else if (strncasecmp(a, "REVALPHA", len) == 0)
            type[i] = 'A';
        else if (strncasecmp(a, "NUM", len) == 0)
            type[i] = 'n';
        else if (strncasecmp(a, "REVNUM", len) == 0)
            type[i] = 'N';
        else
            return RedisModule_ReplyWithError(
                    ctx,
                    "Err: For each column you must tell how to sort with (REV)ALPHA or (REV)NUM");
    }

    RedisModuleCallReply *reply = RedisModule_Call(ctx, "SCARD", "s", set);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER)
        return RedisModule_ReplyWithError(
                ctx,
                "Err: Unable to get the set card");
    int size = RedisModule_CallReplyInteger(reply) * block_size;
    RedisModule_FreeCallReply(reply);

    RedisModuleString **array = RedisModule_Alloc(
            size * sizeof(char *));

    reply = RedisModule_Call(ctx, "SMEMBERS", "s", set);
    for (size_t i = block_size - 1, j = 0; i < size; i += block_size, ++j) {
        size_t len;
        const char *str = RedisModule_CallReplyStringPtr(
                RedisModule_CallReplyArrayElement(reply, j), &len);
        array[i] = RedisModule_CreateString(ctx, str, len);
    }
    RedisModule_FreeCallReply(reply);

    for (size_t i = 0; i < block_size - 1; ++i) {
        for (size_t j = 0; j < size; j += block_size) {
            size_t len;
            reply = RedisModule_Call(
                    ctx, "HGET", "ss",
                    array[j + block_size - 1], argv[4 + 2 * i]);
            const char *str = RedisModule_CallReplyStringPtr(reply, &len);
            array[j + i] = RedisModule_CreateString(ctx, str, len);
            RedisModule_FreeCallReply(reply);
        }
    }

    size_t ldown = first * block_size;
    size_t lup = last * block_size;
    if (ldown >= size)
        ldown = size - block_size;
    if (lup < ldown)
        lup = ldown;
    if (lup >= size)
        lup = size - block_size;

    quick_sort(
            array, type,
            0, size - block_size,
            ldown, lup);

    RedisModule_FreeCallReply(reply);
    reply = RedisModule_Call(ctx,
            "UNLINK", "c", "service_sort");
    RedisModule_FreeCallReply(reply);
    long long w = 0;
    for (size_t i = ldown; i <= lup; i += block_size, ++w) {
        reply = RedisModule_Call(ctx, "ZADD", "cls",
                "services_sort", w, array[i + block_size - 1]);
        RedisModule_FreeCallReply(reply);
    }

    for (size_t i = 0; i < size; ++i) {
        RedisModule_FreeString(ctx, array[i]);
    }
    RedisModule_Free(array);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "tabular", 1, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "tabular.sort",
        TabularSort_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
