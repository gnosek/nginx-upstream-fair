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

/*
 * this must be a power of two as we rely on integer wrap around
 *
 * it should also be small enough to make the whole struct fit in
 * a single cacheline
 */
#define FS_TIME_SLOTS 4

#define FS_UNPADDED_SIZE (2 * sizeof(ngx_atomic_t) + FS_TIME_SLOTS * sizeof(ngx_msec_t))
#define FS_STRUCT_SIZE (ngx_align( FS_UNPADDED_SIZE, CACHELINE_SIZE))

typedef struct {
    ngx_atomic_t                        nreq;
    ngx_atomic_t                        slot;
    volatile ngx_msec_t                 last_active[FS_TIME_SLOTS];
    unsigned char                       padding[FS_STRUCT_SIZE - FS_UNPADDED_SIZE];
} ngx_http_upstream_fair_shared_t;


typedef struct {
    ngx_shm_zone_t                     *shm_zone;
    ngx_http_upstream_fair_shared_t    *shared;
    ngx_http_upstream_rr_peers_t       *rrp;
    ngx_uint_t                         current;
} ngx_http_upstream_fair_peers_t;


#define NGX_PEER_INVALID (~0UL)


typedef struct {
    ngx_http_upstream_rr_peer_data_t   rrpd;
    ngx_http_upstream_fair_shared_t   *shared;
    ngx_http_upstream_fair_peers_t    *peer_data;
    ngx_uint_t                         current;
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
    peers->current = n - 1;

    us->peer.init = ngx_http_upstream_init_fair_peer;

    return NGX_OK;
}


static void
ngx_http_upstream_fair_update_nreq(ngx_http_upstream_fair_peer_data_t *fp, int delta, ngx_log_t *log)
{
    ngx_http_upstream_fair_shared_t     *fs;
    ngx_uint_t                           slot;

    fs = &fp->shared[fp->current];

    ngx_atomic_fetch_add(&fs->nreq, delta);
    slot = ngx_atomic_fetch_add(&fs->slot, 1) % FS_TIME_SLOTS;

    fs->last_active[slot] = ngx_current_msec;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, log, 0, "[upstream_fair] nreq for peer %ui now %d", fp->current, fs->nreq);
}


/*
 * should probably be comparable to average request processing
 * time, including the occasional hogs
 *
 * it's probably better to keep this estimate pessimistic
 */
#define FS_TIME_SCALE_OFFSET 1000

static ngx_int_t
ngx_http_upstream_fair_sched_score(ngx_peer_connection_t *pc,
    ngx_http_upstream_fair_shared_t *fs,
    ngx_http_upstream_rr_peer_t *peer, ngx_uint_t n)
{
    ngx_int_t                           slot;
    ngx_msec_t                          last_active_delta;

    slot = (fs->slot - 1) % FS_TIME_SLOTS;
    last_active_delta = ngx_current_msec - fs->last_active[slot];
    if ((ngx_int_t) last_active_delta < 0) {
        ngx_log_error(NGX_LOG_WARN, pc->log, 0, "[upstream_fair] Clock skew of at least %i msec detected", -(ngx_int_t) last_active_delta);

        /* a pretty arbitrary value */
        last_active_delta = FS_TIME_SCALE_OFFSET;
    }

    /* sanity check */
    if (fs->nreq > INT_MAX) {
        ngx_log_error(NGX_LOG_WARN, pc->log, 0, "[upstream_fair] upstream %ui has negative nreq (%i)", n, fs->nreq);
        return -FS_TIME_SCALE_OFFSET;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] nreq = %i, last_active_delta = %ui", fs->nreq, last_active_delta);

    /*
     * should be pretty unlikely to process a request for many days and still not time out,
     * or to become swamped with requests this heavily; still, we shouldn't drop this backend
     * completely as it wouldn't ever get a chance to recover
     */
    if (fs->nreq > 1 && last_active_delta > 0 && (INT_MAX / ( last_active_delta + FS_TIME_SCALE_OFFSET )) < (fs->nreq - 1)) {
        ngx_log_error(NGX_LOG_WARN, pc->log, 0, "[upstream_fair] upstream %ui has been active for %ul seconds",
            n, last_active_delta / 1000);

        /*
         * schedule behind "sane" backends with the same number of requests pending
         * but (hopefully) before backends with more requests
         */
        return -fs->nreq * FS_TIME_SCALE_OFFSET;
    } else {
        return (1 - fs->nreq) * (last_active_delta + FS_TIME_SCALE_OFFSET);
    }
}

/*
 * the core of load balancing logic
 */

static ngx_int_t
ngx_http_upstream_fair_try_peer(ngx_peer_connection_t *pc,
    ngx_http_upstream_rr_peer_data_t *rrp,
    ngx_uint_t peer_id,
    time_t now)
{
    ngx_uint_t                          n, m;
    ngx_http_upstream_rr_peer_t        *peer;

    n = peer_id / (8 * sizeof(uintptr_t));
    m = (uintptr_t) 1 << peer_id % (8 * sizeof(uintptr_t));

    if (rrp->tried[n] & m)
        return NGX_BUSY;

    peer = &rrp->peers->peer[peer_id];

    if (!peer->down) {
        if (peer->max_fails == 0 || peer->fails < peer->max_fails) {
            return NGX_OK;
        }

        if (now - peer->accessed > peer->fail_timeout) {
            peer->fails = 0;
            return NGX_OK;
        }
    }

    rrp->tried[n] |= m;
    if (pc)
        pc->tries--;
    return NGX_BUSY;
}

static ngx_int_t
ngx_http_upstream_choose_fair_peer(ngx_peer_connection_t *pc,
    ngx_http_upstream_fair_peer_data_t *fp, ngx_uint_t *peer_id)
{
    ngx_uint_t                          i, n;
    ngx_uint_t                          npeers, total_npeers;
    ngx_http_upstream_fair_shared_t     fsc;
    time_t                              now;
    ngx_int_t                           prev_sched_score, sched_score = 0;

    total_npeers = npeers = fp->rrpd.peers->number;

    /* just a single backend */
    if (npeers == 1) {
        *peer_id = 0;
        return NGX_OK;
    }

    now = ngx_time();

    /* any idle backends? */
    for (i = 0, n = fp->current; i < npeers; i++, n = (n + 1) % total_npeers) {
        if (ngx_atomic_fetch_add(&fp->shared[n].nreq, 0) == 0 &&
            ngx_http_upstream_fair_try_peer(pc, &fp->rrpd, n, now) == NGX_OK) {

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] peer %i is idle", n);
            *peer_id = n;
            return NGX_OK;
        }
    }

    /* no idle backends, choose the least loaded one */

    /* skip the nearest failed backends */
    n = fp->current;
    while (npeers && pc->tries) {
        if (ngx_http_upstream_fair_try_peer(pc, &fp->rrpd, n, now) == NGX_OK) {
            break;
        }
        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] backend %d is down, npeers = %d", n, npeers - 1);
        n = (n + 1) % total_npeers;
        npeers--;
    }

    /* all backends down or failed? */
    if (!npeers || !pc->tries) {
        return NGX_BUSY;
    }

    /* calc our current sched score */
    fsc = fp->shared[n];
    prev_sched_score = ngx_http_upstream_fair_sched_score(pc,
        &fsc, &fp->rrpd.peers->peer[n], n);

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] pss = %i (n = %d)", prev_sched_score, n);

    *peer_id = n;

    n = (n + 1) % total_npeers;

    /* calc sched scores for all the peers, until it no longer
     * increases, or we wrap around to the beginning
     */
    for (i = 0; i < npeers; i++, n = (n + 1) % total_npeers) {
        ngx_http_upstream_rr_peer_t *peer;

        if (ngx_http_upstream_fair_try_peer(pc, &fp->rrpd, n, now) != NGX_OK) {
            if (!pc->tries) {
                ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] all backends exhausted");
                return NGX_BUSY;
            }

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] backend %d is dead", n);
            continue;
        }

        peer = &fp->rrpd.peers->peer[n];

        if (peer->current_weight-- == 0) {
            peer->current_weight = peer->weight;
            ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] peer %d expired weight, reset to %d", n, peer->weight);
            continue;
        }

        fsc = fp->shared[n];
        if (i) {
            prev_sched_score = sched_score;
        }
        sched_score = ngx_http_upstream_fair_sched_score(pc, &fsc, peer, n);
        ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] pss = %i, ss = %i (n = %d)", prev_sched_score, sched_score, n);

        if (sched_score <= prev_sched_score)
            return NGX_OK;

        *peer_id = n;
    }

    return NGX_OK;
}

ngx_int_t
ngx_http_upstream_get_fair_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_int_t                           ret;
    ngx_uint_t                          peer_id, i;
    ngx_http_upstream_fair_peer_data_t *fp = data;
    ngx_http_upstream_rr_peer_t        *peer;

    peer_id = fp->current;
    fp->current = (fp->current + 1) % fp->rrpd.peers->number;

    ret = ngx_http_upstream_choose_fair_peer(pc, fp, &peer_id);
    ngx_log_debug(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] fp->current = %d, peer_id = %d, ret = %d",
        fp->current, peer_id, ret);

    if (ret == NGX_BUSY) {
        for (i = 0; i < fp->rrpd.peers->number; i++) {
            fp->rrpd.peers->peer[i].fails = 0;
        }

        pc->name = fp->rrpd.peers->name;
    fp->current = NGX_PEER_INVALID;
    if (pc->tries > 0) {
        pc->tries--;
    }
        return NGX_BUSY;
    }

    /* assert(ret == NGX_OK); */
    peer = &fp->rrpd.peers->peer[peer_id];
    fp->current = peer_id;
    fp->peer_data->current = peer_id;
    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    ngx_http_upstream_fair_update_nreq(data, 1, pc->log);
    return ret;
}


void
ngx_http_upstream_free_fair_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_fair_peer_data_t     *fp = data;
    ngx_http_upstream_rr_peer_t            *peer;
    ngx_uint_t                              weight_delta;

    ngx_log_debug4(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] fp->current = %d, state = %ui, pc->tries = %d, pc->data = %p",
        fp->current, state, pc->tries, pc->data);

    if (fp->current == NGX_PEER_INVALID) {
        return;
    }

    ngx_http_upstream_fair_update_nreq(data, -1, pc->log);

    if (state == 0 && pc->tries == 0) {
        return;
    }

    if (fp->rrpd.peers->number == 1) {
        pc->tries = 0;
    }

    if (state & NGX_PEER_FAILED) {
        peer = &fp->rrpd.peers->peer[fp->current];

        peer->fails++;
        peer->accessed = ngx_time();

        weight_delta = peer->weight / peer->max_fails;

        if (peer->current_weight < weight_delta) {
            peer->current_weight = 0;
        } else {
            peer->current_weight -= weight_delta;
        }
    }
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
        ngx_uint_t i;
        usfp->shared = ngx_slab_alloc(shpool, usfp->rrp->number * sizeof(ngx_http_upstream_fair_shared_t));
        for (i = 0; i < usfp->rrp->number; i++) {
                usfp->shared[i].nreq = 0;
                usfp->shared[i].slot = 1;
                usfp->shared[i].last_active[0] = ngx_current_msec;
        }
    }

    fp->shared = usfp->shared;
    fp->peer_data = usfp;
    fp->current = usfp->current;
    r->upstream->peer.get = ngx_http_upstream_get_fair_peer;
    r->upstream->peer.free = ngx_http_upstream_free_fair_peer;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, "[upstream_fair] peer->tries = %d", r->upstream->peer.tries);

    /* keep the rest of configuration from rr, including e.g. SSL sessions */

    return NGX_OK;
}

