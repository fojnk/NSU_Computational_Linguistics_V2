from neo4j_driver.neo4j_repo import Neo4jRepository


class OntologyRepository:

    def __init__(self, neo_repo: Neo4jRepository):
        self.neo_repo = neo_repo

    def create_class(self, title: str, description: str, parent_uri: str = None):
        node = self.neo_repo.create_node(
            ["Class"], {"title": title, "description": description}
        )
        if parent_uri:
            self.neo_repo.create_arc(node["uri"], parent_uri, "SUBCLASS_OF")
        return node

    def get_class(self, uri: str):
        return self.neo_repo.get_node_by_uri(uri)

    def update_class(self, uri: str, title: str = None, description: str = None):
        updates = {}
        if title:
            updates["title"] = title
        if description:
            updates["description"] = description
        return self.neo_repo.update_node(uri, updates)

    def delete_class(self, uri: str):
        query = """
            MATCH (c:Class {uri:$uri})
            OPTIONAL MATCH (c)<-[:SUBCLASS_OF*0..]-(child:Class)
            OPTIONAL MATCH (o:Object)-[:RDF_TYPE]->(child)
            DETACH DELETE c, child, o
        """
        self.neo_repo.run_custom_query(query, {"uri": uri})

    def get_ontology(self):
        return self.neo_repo.get_all_nodes_and_arcs()

    def get_ontology_parent_classes(self):
        query = """
            MATCH (c:Class)
            WHERE NOT (c)-[:SUBCLASS_OF]->(:Class)
            RETURN c
        """
        return [self.neo_repo._extract_node(rec["c"])
                for rec in self.neo_repo.run_custom_query(query)]

    def get_class_parents(self, class_uri: str):
        query = """
            MATCH (c:Class {uri:$uri})-[:SUBCLASS_OF]->(p:Class)
            RETURN p
        """
        return [self.neo_repo._extract_node(rec["p"])
                for rec in self.neo_repo.run_custom_query(query, {"uri": class_uri})]

    def get_class_children(self, class_uri: str):
        query = """
            MATCH (p:Class {uri:$uri})<-[:SUBCLASS_OF]-(c:Class)
            RETURN c
        """
        return [self.neo_repo._extract_node(rec["c"])
                for rec in self.neo_repo.run_custom_query(query, {"uri": class_uri})]

    def add_class_attribute(self, class_uri: str, title: str):
        prop_node = self.neo_repo.create_node(["DatatypeProperty"], {"title": title})
        self.neo_repo.create_arc(class_uri, prop_node["uri"], "DOMAIN")
        return prop_node

    def delete_class_attribute(self, class_uri: str, title: str):
        query = """
            MATCH (c:Class {uri:$uri})-[:DOMAIN]->(p:DatatypeProperty {title:$title})
            DETACH DELETE p
        """
        self.neo_repo.run_custom_query(query, {"uri": class_uri, "title": title})

    def add_class_object_attribute(self, class_uri: str, title: str, range_class_uri: str):
        prop_node = self.neo_repo.create_node(["ObjectProperty"], {"title": title})
        self.neo_repo.create_arc(class_uri, prop_node["uri"], "DOMAIN")
        self.neo_repo.create_arc(prop_node["uri"], range_class_uri, "RANGE")
        return prop_node

    def delete_class_object_attribute(self, object_property_uri: str):
        query = """
            MATCH (p:ObjectProperty {uri:$uri})
            DETACH DELETE p
        """
        self.neo_repo.run_custom_query(query, {"uri": object_property_uri})

    def collect_signature(self, class_uri: str):
        query = """
            MATCH (c:Class {uri:$uri})
            OPTIONAL MATCH (c)-[:DOMAIN]->(dp:DatatypeProperty)
            OPTIONAL MATCH (c)-[:DOMAIN]->(op:ObjectProperty)-[:RANGE]->(rc:Class)
            WITH collect(distinct {title: dp.title, kind:'datatype'}) as dprops,
                 collect(distinct {title: op.title, kind:'object', range_title: rc.title, range_uri: rc.uri}) as oprops
            RETURN dprops, oprops
        """
        record = self.neo_repo.run_custom_query(query, {"uri": class_uri})[0]
        return {
            "datatype_properties": [r for r in record["dprops"] if r["title"] is not None],
            "object_properties": [r for r in record["oprops"] if r["title"] is not None],
        }

    def create_object(self, class_uri: str, properties: dict, relations: dict = None):
        obj = self.neo_repo.create_node(
            ["Object"],
            {"title": properties.get("title", ""), "description": properties.get("description", "")}
        )
        self.neo_repo.create_arc(obj["uri"], class_uri, "RDF_TYPE")

        # DatatypeProperty values
        for field, value in properties.items():
            if field not in ["title", "description"]:
                prop_node = self.neo_repo.create_node(["Value"], {"title": field, "value": value})
                self.neo_repo.create_arc(obj["uri"], prop_node["uri"], "HAS_VALUE")

        # ObjectProperty values
        if relations:
            for field, target_uri in relations.items():
                self.neo_repo.create_arc(obj["uri"], target_uri, field)

        return obj

    def get_object(self, object_uri: str):
        return self.neo_repo.get_node_by_uri(object_uri)

    def update_object(self, object_uri: str, updates: dict):
        return self.neo_repo.update_node(object_uri, updates)

    def delete_object(self, object_uri: str):
        return self.neo_repo.delete_node_by_uri(object_uri)
