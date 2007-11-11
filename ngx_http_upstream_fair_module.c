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


static void
ngx_http_upstream_fair_update_nreq(ngx_http_upstream_fair_peer_data_t *fp, int delta)
{
    ngx_http_upstream_fair_shared_t     *fs;
    ngx_uint_t                           slot;

    fs = &fp->shared[fp->rrpd.current];

    ngx_atomic_fetch_add(&fs->nreq, delta);
    slot = ngx_atomic_fetch_add(&fs->slot, 1) % FS_TIME_SLOTS;

    fs->last_active[slot] = ngx_current_msec;
}


/*
 * should probably be comparable to average request processing
 * time, including the occasional hogs
 *
 * it's probably better to keep this estimate pessimistic
 */
#define FS_TIME_SCALE_OFFSET 1000

static ngx_inline
ngx_int_t ngx_http_upstream_fair_peer_is_down(
    ngx_http_upstream_rr_peer_t *peer, time_t now)
{
    if (peer->down) {
        return 1;
    }

    if (peer->max_fails == 0
        || peer->fails < peer->max_fails)
    {
        return 0;
    }

    if (now - peer->accessed > peer->fail_timeout) {
        peer->fails = 0;
        return 0;
    }

    return 1;
}

static ngx_int_t
ngx_http_upstream_fair_sched_score(ngx_peer_connection_t *pc,
    ngx_http_upstream_fair_shared_t *fs,
    ngx_http_upstream_rr_peer_t *peer)
{
    ngx_int_t                           slot;
    ngx_msec_t                          last_active_delta;

    slot = (fs->slot - 1) % FS_TIME_SLOTS;
    last_active_delta = ngx_current_msec - fs->last_active[slot];
    if ((ngx_int_t) last_active_delta < 0) {
        ngx_log_error(NGX_LOG_WARN, pc->log, 0, "[rr ] Clock skew of at least %i msec detected", -(ngx_int_t) last_active_delta);

        /* a pretty arbitrary value */
        last_active_delta = FS_TIME_SCALE_OFFSET;
    }

    /*
     * should be pretty unlikely to process a request for many days and still not time out,
     * or to become swamped with requests this heavily; still, we shouldn't drop this backend
     * completely as it wouldn't ever get a chance to recover
     */
    if (fs->nreq > 1 && last_active_delta > 0 && (INT_MAX / ( last_active_delta + FS_TIME_SCALE_OFFSET )) < fs->nreq - 1) {
        ngx_log_error(NGX_LOG_WARN, pc->log, 0, "Upstream %V has been active for %lu seconds",
            &peer->name, last_active_delta / 1000);

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
 * the two methods below are the core of load balancing logic
 */

ngx_int_t
ngx_http_upstream_get_fair_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_int_t                           ret;
    ngx_http_upstream_fair_peer_data_t *fp = data;
    ngx_http_upstream_fair_shared_t    *fs;
    ngx_http_upstream_fair_shared_t     fsc;

    ngx_int_t                           first_sched_score = INT_MIN;
    ngx_int_t                           sched_score = INT_MIN;
    ngx_int_t                           prev_sched_score = INT_MIN;
    time_t                              now;
    ngx_uint_t                          i;
    ngx_uint_t                          first_peer;
    ngx_uint_t                          next_peer;

    /*
     * ngx_http_upstream_rr_peer_data_t is the first member,
     * so just passing data is safe
     */

    ret = ngx_http_upstream_get_round_robin_peer(pc, data);
    if (ret != NGX_OK) {
        return ret;
    }

    now = ngx_time();

    first_peer = fp->rrpd.current;
    for (i = 0; i < fp->rrpd.peers->number; i++) {
        fs = &fp->shared[fp->rrpd.current];

        /* the fast path -- current backend is idle */
        if (ngx_atomic_fetch_add(&fs->nreq, 0) == 0)
            break;

        /* keep a local copy -- TODO: proper locking */
        fsc = *fs;

        /* calculate scheduler score for currently selected backend */
        sched_score = ngx_http_upstream_fair_sched_score(pc, &fsc,
            &fp->rrpd.peers->peer[fp->rrpd.current]);

        /* keep the first score for logging purposes */
        if (!i) {
            first_sched_score = sched_score;
        }

        /*
         * the new score isn't any better than the old one, roll back to the
         * previous backend
         *
         * we roll back also when the score is equal to keep close to the round
         * robin model where it makes sense
         */
        if (sched_score <= prev_sched_score) {
            ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] i=%i, %i <= %i, rolling back", i, sched_score, prev_sched_score);

            /* the last change was bad, roll it back */
            sched_score = prev_sched_score;
            if (fp->rrpd.current == 0) {
                fp->rrpd.current = fp->rrpd.peers->number - 1;
            } else {
                fp->rrpd.current--;
            }
            /* don't look any further for two reasons:
             * 1. as we're "almost" round robin, it's unlikely that there are much
             *    better backends futher down the list
             * 2. we don't want the cache lines to bounce too much (they're already
             *    quite hot)
             */
            break;
        }

        /* check the next peer */
        next_peer = fp->rrpd.current + 1;
        if (next_peer >= fp->rrpd.peers->number) {
            next_peer = 0;
        }
        fp->rrpd.current = next_peer;
        prev_sched_score = sched_score;

        if (ngx_http_upstream_fair_peer_is_down(&fp->rrpd.peers->peer[fp->rrpd.current], now)) {
            /* should probably use fancier logic, but for now it'll do */
            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] next peer %ui is down", next_peer);
            break;
        }
    }

    /*
     * tell the upstream core to use the new backend
     * and advance the "current peer" for the next run of round-robin
     */
    if (first_peer != fp->rrpd.current) {
        ngx_http_upstream_rr_peer_t *orig_peer = &fp->rrpd.peers->peer[first_peer];
        ngx_http_upstream_rr_peer_t *peer = &fp->rrpd.peers->peer[fp->rrpd.current];

        /* undo ajdustments made to the original peer by rr algorithm */
        if (orig_peer->current_weight == orig_peer->weight) {
            orig_peer->current_weight = 1;
        } else {
            orig_peer->current_weight++;
        }

        /* adjust the new peer accordingly */
        if (peer->current_weight == 1) {
            peer->current_weight = peer->weight;
        } else {
            peer->current_weight--;
        }

        pc->sockaddr = peer->sockaddr;
        pc->socklen = peer->socklen;
        pc->name = &peer->name;

        ngx_log_debug5(NGX_LOG_DEBUG_HTTP, pc->log, 0, "[upstream_fair] sched_score [%ui] %i -> [%ui] %i in %ui iterations",
            first_peer, first_sched_score, fp->rrpd.current, sched_score, i);
    }

    ngx_http_upstream_fair_update_nreq(data, 1);
    return ret;
}


void
ngx_http_upstream_free_fair_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_fair_update_nreq(data, -1);
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
        ngx_uint_t i;
        time_t now = ngx_time();
        usfp->shared = ngx_slab_alloc(shpool, usfp->rrp->number * sizeof(ngx_http_upstream_fair_shared_t));
        for (i = 0; i < usfp->rrp->number; i++) {
                usfp->shared[i].nreq = 0;
                usfp->shared[i].slot = 0;
                usfp->shared[i].last_active[0] = now;
        }
    }

    if (usfp->shared) {
        fp->shared = usfp->shared;
        r->upstream->peer.get = ngx_http_upstream_get_fair_peer;
        r->upstream->peer.free = ngx_http_upstream_free_fair_peer;
    } /* else just fallback on round-robin behaviour */

    /* keep the rest of configuration from rr, including e.g. SSL sessions */

    return NGX_OK;
}

