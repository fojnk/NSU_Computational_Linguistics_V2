import uuid
import json
from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship
from neo4j.exceptions import Neo4jError


class TArc(dict):
    id: int
    uri: str
    node_uri_from: str
    node_uri_to: str


class TNode(dict):
    id: int
    uri: str
    labels: list
    properties: dict
    arcs: list


class Neo4jRepository:

    def __init__(self, uri, user, password):
        self._driver = None
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            self._driver = driver
            print("Successfully connected to the Neo4j database.")
        except Neo4jError as error:
            print(f"Failed to connect: {error}")

    def close(self):
        if self._driver:
            self._driver.close()
            print("Database connection closed.")

    @staticmethod
    def generate_random_string(length=20):
        return uuid.uuid4().hex[:length]

    @staticmethod
    def _extract_node(node_obj: Node):
        return {
            "id": node_obj.element_id,
            "uri": node_obj.get("uri"),
            "labels": list(node_obj.labels),
            "properties": dict(node_obj.items()),
        }

    @staticmethod
    def _extract_arc(relationship: Relationship):
        return {
            "id": relationship.element_id,
            "uri": relationship.type,
            "node_uri_from": relationship.start_node.get("uri"),
            "node_uri_to": relationship.end_node.get("uri"),
        }

    def transform_labels(self, labels, separator=':'):
        if len(labels) == 0:
            return '``'
        res = ''
        for l in labels:
            res += f'`{l}`' + separator
        return res[:-1]

    def transform_props(self, props):
        if len(props) == 0:
            return ''
        data = '{'
        for p in props:
            data += f'`{p}`:{json.dumps(props[p])},'
        data = data[:-1]
        data += '}'
        return data


    def create_node(self, labels, properties):
        properties["uri"] = self.generate_random_string()
        labels_formatted = ''
        if labels:
            labels_formatted = ':' + self.transform_labels(labels, separator=':')

        query = f"CREATE (a{labels_formatted} $props) RETURN a"
        with self._driver.session() as session:
            result = session.run(query, props=properties).single()
            if result:
                return self._extract_node(result["a"])
            return None

    def get_nodes_by_labels(self, labels):
        if not labels:
            return []
        label_string = ':' + self.transform_labels(labels, separator=':')
        query = f"MATCH (a{label_string}) RETURN a"
        with self._driver.session() as session:
            results = session.run(query)
            return [self._extract_node(record["a"]) for record in results]



    def _fetch_nodes_with_arcs(self, session, uris=None):
        if uris:
            cypher = """
                MATCH (a) WHERE a.uri IN $uris
                OPTIONAL MATCH (a)-[r]->()
                RETURN a, collect(r) AS arcs
            """
            result = session.run(cypher, uris=uris)
        else:
            cypher = """
                MATCH (a)
                OPTIONAL MATCH (a)-[r]->()
                RETURN a, collect(r) AS arcs
            """
            result = session.run(cypher)

        nodes_dict = {}
        for record in result:
            a = record["a"]
            node_uri = a.get("uri")
            if node_uri not in nodes_dict:
                nodes_dict[node_uri] = self._extract_node(a)
                nodes_dict[node_uri]["arcs"] = []

            for rel in record["arcs"]:
                if rel:
                    nodes_dict[node_uri]["arcs"].append(self._extract_arc(rel))

        return list(nodes_dict.values())

    def get_all_nodes(self):
        query = "MATCH (a) RETURN a"
        with self._driver.session() as session:
            records = session.run(query)
            return [self._extract_node(rec["a"]) for rec in records]

    def get_all_nodes_and_arcs(self):
        with self._driver.session() as session:
            return self._fetch_nodes_with_arcs(session)

    def get_node_by_uri(self, uri):
        with self._driver.session() as session:
            nodes = self._fetch_nodes_with_arcs(session, uris=[uri])
            if nodes:
                return nodes[0]
            return None

    def create_arc(self, node1_uri, node2_uri, rel_type):
        query = f"""
            MATCH (a {{uri: $uri1}}), (b {{uri: $uri2}})
            CREATE (a)-[r:`{rel_type}`]->(b)
            RETURN r, a, b
        """
        with self._driver.session() as session:
            result = session.run(query, uri1=node1_uri, uri2=node2_uri).single()
            if result:
                rel = result["r"]
                start_node = result["a"]
                end_node = result["b"]
                return {
                    "id": rel.element_id,
                    "uri": rel.type,
                    "node_uri_from": start_node.get("uri"),
                    "node_uri_to": end_node.get("uri"),
                }
            return None

    def delete_node_by_uri(self, uri):
        cypher = """
            MATCH (n {uri: $uri})
            DETACH DELETE n
            RETURN count(n) as deleted_count
        """
        with self._driver.session() as session:
            result = session.run(cypher, uri=uri).single()
            if result:
                return result["deleted_count"] > 0
            return False

    def delete_arc_by_id(self, arc_id):
        cypher = """
            MATCH ()-[r]-() WHERE elementId(r) = $id
            DELETE r
            RETURN count(r) as deleted_count
        """
        with self._driver.session() as session:
            result = session.run(cypher, id=str(arc_id)).single()
            if result:
                return result["deleted_count"] > 0
            return False

    def update_node(self, uri, params_to_update):
        if not params_to_update:
            return self.get_node_by_uri(uri)
        cypher = """
            MATCH (n {uri: $uri})
            SET n += $updates
            RETURN n
        """
        with self._driver.session() as session:
            result = session.run(cypher, uri=uri, updates=params_to_update).single()
            if result:
                return self._extract_node(result["n"])
            return None

    def run_custom_query(self, query, params=None):
        params = params or {}
        with self._driver.session() as session:
            result = session.run(query, **params)
            return [record for record in result]