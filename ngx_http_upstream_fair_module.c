/*
 * Copyright (C) 2007 Grzegorz Nosek
 * Work sponsored by Ezra Zygmuntowicz & EngineYard.com
 *
 * Based on nginx source (C) Igor Sysoev
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#define CACHELINE_SIZE 64

typedef struct {
    ngx_atomic_t                        lock;
    ngx_msec_t                          last_active;
    int                                 nreq;
    unsigned char                       padding[ CACHELINE_SIZE - sizeof(ngx_atomic_t) - sizeof(ngx_msec_t) - sizeof(int) ];
} ngx_http_upstream_fair_shared_t;


typedef struct {
    ngx_shm_zone_t                     *shm_zone;
    ngx_http_upstream_fair_shared_t    *shared;
    ngx_http_upstream_rr_peers_t       *rrp;
} ngx_http_upstream_fair_peers_t;


typedef struct {
    ngx_http_upstream_rr_peer_data_t   rrpd;
    ngx_http_upstream_fair_shared_t   *shared;
} ngx_http_upstream_fair_peer_data_t;


static ngx_int_t ngx_http_upstream_fair_init_module(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_upstream_init_fair(ngx_conf_t *cf,
    ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_fair_peer(ngx_peer_connection_t *pc,
    void *data);
static void ngx_http_upstream_free_fair_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static ngx_int_t ngx_http_upstream_init_fair_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);
static char *ngx_http_upstream_fair(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);


static ngx_command_t  ngx_http_upstream_fair_commands[] = {

    { ngx_string("fair"),
      NGX_HTTP_UPS_CONF|NGX_CONF_NOARGS,
      ngx_http_upstream_fair,
      0,
      0,
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_upstream_fair_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    NULL,                                  /* create location configuration */
    NULL                                   /* merge location configuration */
};


ngx_module_t  ngx_http_upstream_fair_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_fair_module_ctx, /* module context */
    ngx_http_upstream_fair_commands,    /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_http_upstream_fair_init_shm_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    return NGX_OK;
}

static char *
ngx_http_upstream_fair(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t  *uscf;

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    uscf->peer.init_upstream = ngx_http_upstream_init_fair;

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE
                  |NGX_HTTP_UPSTREAM_WEIGHT
                  |NGX_HTTP_UPSTREAM_MAX_FAILS
                  |NGX_HTTP_UPSTREAM_FAIL_TIMEOUT
                  |NGX_HTTP_UPSTREAM_DOWN;

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_upstream_init_fair(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_fair_peers_t     *peers;
    ngx_uint_t                          n;

    /* do the dirty work using rr module */
    if (ngx_http_upstream_init_round_robin(cf, us) != NGX_OK) {
        return NGX_ERROR;
    }

    /* setup our wrapper around rr */
    peers = ngx_palloc(cf->pool, sizeof *peers);
    if (peers == NULL) {
        return NGX_ERROR;
    }
    peers->rrp = us->peer.data;
    us->peer.data = peers;
    n = peers->rrp->number;

    peers->shm_zone = ngx_shared_memory_add(cf, peers->rrp->name, 8 * ngx_pagesize, &ngx_http_upstream_fair_module);
    if (peers->shm_zone == NULL) {
        return NGX_ERROR;
    }
    peers->shm_zone->init = ngx_http_upstream_fair_init_shm_zone;
    peers->shared = NULL;

    us->peer.init = ngx_http_upstream_init_fair_peer;

    return NGX_OK;
}

/*
 * the two methods below are the core of load balancing logic
 *
 * for now, just pass through to round robin
 */

ngx_int_t
ngx_http_upstream_get_fair_peer(ngx_peer_connection_t *pc, void *data)
{
    /*
     * ngx_http_upstream_rr_peer_data_t is the first member,
     * so just passing data is safe
     */

    return ngx_http_upstream_get_round_robin_peer(pc, data);
}


void
ngx_http_upstream_free_fair_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_free_round_robin_peer(pc, data, state);
}


ngx_int_t
ngx_http_upstream_init_fair_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_http_upstream_fair_peer_data_t     *fp;
    ngx_http_upstream_fair_peers_t         *usfp;
    ngx_slab_pool_t                        *shpool;

    fp = r->upstream->peer.data;

    if (fp == NULL) {
        fp = ngx_palloc(r->pool, sizeof(ngx_http_upstream_fair_peer_data_t));
        if (fp == NULL) {
            return NGX_ERROR;
        }

        r->upstream->peer.data = fp;
    }

    usfp = us->peer.data; /* hide our wrapper from rr */
    us->peer.data = usfp->rrp;

    if (ngx_http_upstream_init_round_robin_peer(r, us) != NGX_OK) {
        return NGX_ERROR;
    }

    /* restore saved usfp pointer */
    us->peer.data = usfp;

    /* set up shared memory area */
    shpool = (ngx_slab_pool_t *)usfp->shm_zone->shm.addr;

    if (!usfp->shared) {
        usfp->shared = ngx_slab_alloc(shpool, usfp->rrp->number * sizeof(ngx_http_upstream_fair_shared_t));
    }

    if (usfp->shared) {
        fp->shared = usfp->shared;
        r->upstream->peer.get = ngx_http_upstream_get_fair_peer;
        r->upstream->peer.free = ngx_http_upstream_free_fair_peer;
    } /* else just fallback on round-robin behaviour */

    /* keep the rest of configuration from rr, including e.g. SSL sessions */

    return NGX_OK;
}

