define show_fair_peer
	set $n = (ngx_http_upstream_fair_shm_block_t *)$arg0
	set $peers = $n->peers
	set $rr_peers = $peers->rrp
	printf "upstream id: 0x%08x, current peer: %d/%d\n", $n->node.key, $peers->current, $rr_peers->number
	set $i = 0
	while $i < $rr_peers->number
		printf "peer %d: %s weight: %d/%d fails: %d/%d acc: %d down: %d nreq: %u last_act: %u\n", $i, $rr_peers->peer[$i].name.data,\
			$rr_peers->peer[$i].current_weight, $rr_peers->peer[$i].weight,\
			$rr_peers->peer[$i].fails, $rr_peers->peer[$i].max_fails,\
			$rr_peers->peer[$i].accessed, $rr_peers->peer[$i].down,\
			$n->stats[$i].nreq, $n->stats[$i].last_active
		set $i = $i + 1
	end
	printf "-----------------\n"
	if ($n->node.left != $arg1)
		show_fair_peer $n->node.left $arg1
	end
	if ($n->node.right != $arg1)
		show_fair_peer $n->node.right $arg1
	end
end

define show_fair_peers
	set $tree = ngx_http_upstream_fair_rbtree
	if (!$tree)
		printf "Cannot find the upstream_fair peer information tree\n"
	else
		set $root = (ngx_http_upstream_fair_shm_block_t *)($tree->root)
		if ($root != $tree->sentinel)
			show_fair_peer $root $tree->sentinel
		else
			printf "No upstream_fair peer information\n"
		end
	end
end
document show_fair_peers
Dump upstream_fair peer infromation
end
