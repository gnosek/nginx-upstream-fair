define show_fair_peer
	set $n = (ngx_http_upstream_fair_shm_block_t *)$arg0
	set $peers = $n->peers
	printf "upstream id: 0x%08x, current peer: %d/%d\n", $n->node.key, $peers->current, $peers->number
	set $i = 0
	while $i < $peers->number
		printf "peer %d: %s weight: %d/%d fails: %d/%d acc: %d down: %d nreq: %u last_act: %u\n", $i, $peers->peer[$i].name.data,\
			$peers->peer[$i].current_weight, $peers->peer[$i].weight,\
			$peers->peer[$i].fails, $peers->peer[$i].max_fails,\
			$peers->peer[$i].accessed, $peers->peer[$i].down,\
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
