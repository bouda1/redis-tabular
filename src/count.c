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
#include <string.h>
#include "count.h"
#include "tabular.h"

static CountList *FillList(CountList *lst, RedisModuleString *content) {
    if (lst->content == NULL)
        lst->content = content;
    else
        while (RedisModule_StringCompare(lst->content, content) != 0) {
            if (lst->next) {
                lst = lst->next;
            }
            else {
                lst->next = RedisModule_Alloc(sizeof(CountList));
                lst = lst->next;
                memset(lst, 0, sizeof(CountList));
                lst->content = content;
                break;
            }
        }

    lst->count++;
    if (lst->children)
        lst = lst->children;
    else {
        lst->children = RedisModule_Alloc(sizeof(CountList));
        lst = lst->children;
        memset(lst, 0, sizeof(CountList));
    }
    return lst;
}

CountList *Count(RedisModuleCtx *ctx, RedisModuleString **array, int size,
                 TabularHeader *header, int block_size) {
    CountList *retval = RedisModule_Alloc(sizeof(CountList));
    memset(retval, 0, sizeof(CountList));
    for (int i = 0; i < size; i += block_size) {
        CountList *lst = retval;
        int cont = 1;
        for (int j = 0; cont && j < block_size - 1; ++j) {
            size_t len;
            const char *txt;
            switch (header[j].tool) {
                case TABULAR_MATCH:
                    txt = RedisModule_StringPtrLen(array[i + j], &len);
                    if (fnmatch(header[j].search, txt, FNM_NOESCAPE | FNM_CASEFOLD | FNM_EXTMATCH) == 0)
                        lst = FillList(lst, array[i + j]);
                    break;
                case TABULAR_EQUAL:
                    txt = RedisModule_StringPtrLen(array[i + j], &len);
                    if (strncmp(header[j].search, txt, len) == 0)
                        lst = FillList(lst, array[i + j]);
                    break;
                default:
                    cont = 0;
            }
        }
    }
    return retval;
}

void CountReply(RedisModuleCtx *ctx, CountList *cnt) {
    if (cnt->content == NULL) {
        RedisModule_ReplyWithNull(ctx);
        return;
    }

    int s = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (CountList *lst = cnt; lst; lst = lst->next) {
        RedisModule_ReplyWithStringBuffer(ctx, "value", 5);
        RedisModule_ReplyWithString(ctx, lst->content);
        RedisModule_ReplyWithStringBuffer(ctx, "count", 5);
        RedisModule_ReplyWithLongLong(ctx, lst->count);
        RedisModule_ReplyWithStringBuffer(ctx, "children", 8);
        if (lst->children && lst->children->content) {
            CountReply(ctx, lst->children);
        }
        else
            RedisModule_ReplyWithNull(ctx);
        s += 6;
    }
    RedisModule_ReplySetArrayLength(ctx, s);
}

void CountReplyStore(RedisModuleCtx *ctx, CountList *cnt, RedisModuleString *store) {
    if (cnt->content == NULL) {
        RedisModule_ReplyWithNull(ctx);
        return;
    }

    for (CountList *lst = cnt; lst; lst = lst->next) {
        RedisModuleString *tmp = RedisModule_CreateStringFromString(ctx, store);
        RedisModule_StringAppendBuffer(ctx, tmp, ":", 1);
        RedisModule_StringAppendBuffer(ctx, tmp, "count", 5);
        for (CountList *c = lst; c && c->content; c = c->children) {
            size_t len;
            const char *str = RedisModule_StringPtrLen(c->content, &len);
            RedisModule_StringAppendBuffer(ctx, tmp, ":", 1);
            RedisModule_StringAppendBuffer(
                    ctx, tmp, str, len);
            RedisModuleKey *key = RedisModule_OpenKey(ctx, tmp, REDISMODULE_WRITE);
            RedisModuleString *val = RedisModule_CreateStringFromLongLong(
                    ctx, lst->count);
            RedisModule_StringSet(key, val);
            RedisModule_CloseKey(key);
            RedisModule_FreeString(ctx, val);
        }
        RedisModule_FreeString(ctx, tmp);
    }
    RedisModule_ReplyWithSimpleString(ctx, "OK");
}

void FreeCountList(CountList *cnt) {
    CountList *next = cnt->next;
    CountList *children = cnt->children;

    RedisModule_Free(cnt);
    if (next)
        FreeCountList(next);
    if (children)
        FreeCountList(children);
}
