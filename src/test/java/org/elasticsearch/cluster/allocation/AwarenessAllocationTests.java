/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.allocation;

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ClusterScope(scope= ElasticsearchIntegrationTest.Scope.TEST, numDataNodes =0)
public class AwarenessAllocationTests extends ElasticsearchIntegrationTest {

    private final ESLogger logger = Loggers.getLogger(AwarenessAllocationTests.class);

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Test
    public void testSimpleAwareness() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.schedule", "10ms")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build();


        logger.info("--> starting 2 nodes on the same rack");
        internalCluster().startNodesAsync(2, ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1").build()).get();

        createIndex("test1");
        createIndex("test2");

        NumShards test1 = getNumShards("test1");
        NumShards test2 = getNumShards("test2");
        //no replicas will be allocated as both indices end up on a single node
        final int totalPrimaries = test1.numPrimaries + test2.numPrimaries;

        ensureGreen();

        logger.info("--> starting 1 node on a different rack");
        final String node3 = internalCluster().startNode(ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_2").build());

        // On slow machines the initial relocation might be delayed
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {

                logger.info("--> waiting for no relocation");
                ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").setWaitForRelocatingShards(0).get();
                if (clusterHealth.isTimedOut()) {
                    return false;
                }

                logger.info("--> checking current state");
                ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
                // verify that we have all the primaries on node3
                ObjectIntOpenHashMap<String> counts = new ObjectIntOpenHashMap<>();
                for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
                    for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                        for (ShardRouting shardRouting : indexShardRoutingTable) {
                            counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                        }
                    }
                }
                return counts.get(node3) == totalPrimaries;
            }
        }, 10, TimeUnit.SECONDS), equalTo(true));
    }
    
    @Test
    @Slow
    public void testAwarenessZones() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .build();

        logger.info("--> starting 4 nodes on different zones");
        List<String> nodes = internalCluster().startNodesAsync(
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "a").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "a").build()
        ).get();
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        String B_1 = nodes.get(2);
        String A_1 = nodes.get(3);
        client().admin().indices().prepareCreate("test")
        .setSettings(settingsBuilder().put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)).execute().actionGet();
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        ObjectIntOpenHashMap<String> counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        assertThat(counts.get(A_1), anyOf(equalTo(2),equalTo(3)));
        assertThat(counts.get(B_1), anyOf(equalTo(2),equalTo(3)));
        assertThat(counts.get(A_0), anyOf(equalTo(2),equalTo(3)));
        assertThat(counts.get(B_0), anyOf(equalTo(2),equalTo(3)));
    }
    
    @Test
    @Slow
    public void testAwarenessZonesIncrementalNodes() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .build();

        logger.info("--> starting 2 nodes on zones 'a' & 'b'");
        List<String> nodes = internalCluster().startNodesAsync(
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "a").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").build()
        ).get();
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        client().admin().indices().prepareCreate("test")
        .setSettings(settingsBuilder().put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)).execute().actionGet();
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        ObjectIntOpenHashMap<String> counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(5));
        logger.info("--> starting another node in zone 'b'");

        String B_1 = internalCluster().startNode(ImmutableSettings.settingsBuilder().put(commonSettings).put("node.zone", "b").build());
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        client().admin().cluster().prepareReroute().get();
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("3").setWaitForActiveShards(10).setWaitForRelocatingShards(0).execute().actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        
        String noZoneNode = internalCluster().startNode();
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        client().admin().cluster().prepareReroute().get();
        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").setWaitForActiveShards(10).setWaitForRelocatingShards(0).execute().actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        
        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.containsKey(noZoneNode), equalTo(false));
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.settingsBuilder().put("cluster.routing.allocation.awareness.attributes", "").build()).get();

        health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("4").setWaitForActiveShards(10).setWaitForRelocatingShards(0).execute().actionGet();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new ObjectIntOpenHashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.addTo(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1);
                }
            }
        }
        
        assertThat(counts.get(A_0), equalTo(3));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.get(noZoneNode), equalTo(2));
    }

    @Test
    @Slow
    public void testRackAwarenessBalance() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build();

        logger.info("--> starting 20 nodes on different racks");
        final List<String> nodes = internalCluster().startNodesAsync(
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "k02").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "k04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "k04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "k04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "k04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "k04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l02").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l02").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l02").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l03").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l03").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l04").build(),
                ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "l05").build()
        ).get();
        Map<String, String> names = new HashMap<String, String>(20) {{
            put(nodes.get(0), "k02_1");
            put(nodes.get(1), "k04_1");
            put(nodes.get(2), "k04_2");
            put(nodes.get(3), "k04_3");
            put(nodes.get(4), "k04_4");
            put(nodes.get(5), "k04_5");
            put(nodes.get(6), "l02_1");
            put(nodes.get(7), "l02_2");
            put(nodes.get(8), "l02_3");
            put(nodes.get(9), "l03_1");
            put(nodes.get(10), "l03_2");
            put(nodes.get(11), "l04_1");
            put(nodes.get(12), "l04_2");
            put(nodes.get(13), "l04_3");
            put(nodes.get(14), "l04_4");
            put(nodes.get(15), "l04_5");
            put(nodes.get(16), "l04_6");
            put(nodes.get(17), "l04_7");
            put(nodes.get(18), "l04_8");
            put(nodes.get(19), "l05_1");
        }};

        logger.info("--> making index with 40 shards (5 primary, 7 replicas)");
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 7)
                .put("index.routing.allocation.total_shards_per_node", 2))
                .execute().actionGet();
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus().setWaitForNodes("20").setTimeout("30s").setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        ObjectIntOpenHashMap<String> perNodeCounts = new ObjectIntOpenHashMap<>();
        ObjectIntOpenHashMap<String> perRackCounts = new ObjectIntOpenHashMap<>();

        System.out.println("*****************");
        System.out.println("Shard allocation:");

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (!shardRouting.assignedToNode()) {
                        continue;
                    }
                    DiscoveryNode node = clusterState.nodes().get(shardRouting.currentNodeId());
                    String name = names.get(node.name());
                    perNodeCounts.addTo(name, 1);
                    perRackCounts.addTo(node.getAttributes().get("rack_id"), 1);

                    System.out.println("shard " + shardRouting.getId() + " on " + name);
                }
            }
        }

        System.out.println("===============");
        System.out.println("Shards per rack:");

        int shardSum1 = 0;
        for (ObjectIntCursor<String> cursor : perRackCounts) {
            String rackId = cursor.key;
            int numShardsOnRack = perRackCounts.get(rackId);
            shardSum1 += numShardsOnRack;
            System.out.println(rackId + " " + numShardsOnRack);
        }

        System.out.println("---------------");
        System.out.println("Shards per node:");

        int shardSum2 = 0;
        for (ObjectIntCursor<String> cursor : perNodeCounts) {
            String nodeName = cursor.key;
            int numShardsOnNode = perNodeCounts.get(nodeName);
            shardSum2 += numShardsOnNode;
            System.out.println(nodeName + " " + numShardsOnNode);
            assertThat("allocation not balanced looking at number of shards on " + nodeName, numShardsOnNode, equalTo(2));
        }

        assertThat("not all shards assigned", shardSum1, equalTo(40));
        assertThat("not all shards assigned", shardSum2, equalTo(40));
    }

}
