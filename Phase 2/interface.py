from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(
            uri,
            auth=(user, password),
            encrypted=False,
            database="neo4j"
        )
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()
    
    def bfs(self, start_node, target_nodes):
        if not isinstance(target_nodes, list):
            target_nodes = [target_nodes]

        with self._driver.session() as session:
            session.run("""
            CALL gds.graph.project(
                'bfs_graph',
                'Location',
                {
                    TRIP: {
                        type: 'TRIP',
                        orientation: 'NATURAL'
                    }
                }
            )
            """)

            node_info = session.run("""
            MATCH (start:Location {name: $start_node})
            MATCH (target:Location) WHERE target.name IN $target_nodes
            RETURN id(start) AS sourceId, collect(id(target)) AS targetIds
            """, start_node=start_node, target_nodes=target_nodes).single()

            if not node_info:
                return []

            results = session.run("""
            CALL gds.bfs.stream(
                'bfs_graph',
                {
                    sourceNode: $sourceId,
                    targetNodes: $targetIds
                }
            )
            YIELD path
            RETURN path
            """, sourceId=node_info["sourceId"], targetIds=node_info["targetIds"]).data()

            session.run("CALL gds.graph.drop('bfs_graph')")

        return results
    
    def pagerank(self, max_iterations, weight_property):
        with self._driver.session() as session:
            session.run("""
            CALL gds.graph.project(
                'pagerank_graph',
                'Location',
                {
                    TRIP: {
                        type: 'TRIP',
                        orientation: 'NATURAL',
                        properties: {
                            weight: {
                                property: $weight_property,
                                defaultValue: 1.0
                            }
                        }
                    }
                }
            )
            """, weight_property=weight_property)

            results = session.run("""
            CALL gds.pageRank.stream(
                'pagerank_graph',
                {
                    maxIterations: $max_iterations,
                    dampingFactor: 0.85,
                    relationshipWeightProperty: 'weight'
                }
            )
            YIELD nodeId, score
            RETURN {name: gds.util.asNode(nodeId).name, score: score} AS result
            ORDER BY score DESC
            """, max_iterations=max_iterations).data()

            session.run("CALL gds.graph.drop('pagerank_graph', false)")

        if not results:
            return [{'name': None, 'score': None}, {'name': None, 'score': None}]

        return [results[0]['result'], results[-1]['result']]
