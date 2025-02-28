# Check that the no-failover option works

source tests/support/cluster.tcl

start_cluster 3 3 {tags {external:skip cluster}} {

test "Cluster is up" {
    wait_for_cluster_state ok
}

test "Instance #3 is a replica" {
    assert {[s -3 role] eq {slave}}

    # Configure it to never failover the master
    R 3 CONFIG SET cluster-replica-no-failover yes
}

test "Instance #3 synced with the master" {
    wait_for_condition 1000 50 {
        [s -3 master_link_status] eq {up}
    } else {
        fail "Instance #3 master link status is not up"
    }
}

test "The nofailover flag is propagated" {
    set replica3_id [dict get [cluster_get_myself 3] id]

    for {set j 0} {$j < [llength $::servers]} {incr j} {
        wait_for_condition 1000 50 {
            [cluster_has_flag [cluster_get_node_by_id $j $replica3_id] nofailover]
        } else {
            fail "Instance $id can't see the nofailover flag of replica"
        }
    }
}

test "Killing one master node" {
    pause_process [srv 0 pid]
}

test "Cluster should be still down after some time" {
    wait_for_condition 1000 50 {
        [CI 1 cluster_state] eq {fail} &&
        [CI 2 cluster_state] eq {fail} &&
        [CI 3 cluster_state] eq {fail} &&
        [CI 4 cluster_state] eq {fail} &&
        [CI 5 cluster_state] eq {fail}
    } else {
        fail "Cluster doesn't fail"
    }
}

test "Instance #3 is still a replica" {
    assert {[s -3 role] eq {slave}}
}

test "Restarting the previously killed master node" {
    resume_process [srv 0 pid]
}

} ;# start_cluster
