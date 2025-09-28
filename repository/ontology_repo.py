from neo4j_driver.neo4j_repo import Neo4jRepository


class OntologyRepository:

    def __init__(self, neo_repo: Neo4jRepository):
        self.neo_repo = neo_repo

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

    def get_class(self, uri: str):
        return self.neo_repo.get_node_by_uri(uri)

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

    def get_class_objects(self, class_uri: str):
        query = """
            MATCH (o:Object {class_uri:$uri})
            RETURN o
        """
        return [self.neo_repo._extract_node(rec["o"])
                for rec in self.neo_repo.run_custom_query(query, {"uri": class_uri})]

    def update_class(self, uri: str, title: str = None, description: str = None):
        updates = {}
        if title: updates["title"] = title
        if description: updates["description"] = description
        return self.neo_repo.update_node(uri, updates)

    def create_class(self, title: str, description: str, parent_uri: str = None):
        node = self.neo_repo.create_node(
            ["Class"],
            {"title": title, "description": description}
        )
        if parent_uri:
            self.neo_repo.create_arc(node["uri"], parent_uri, "SUBCLASS_OF")
        return node

    def delete_class(self, uri: str):
        query = """
            MATCH (c:Class {uri:$uri})
            OPTIONAL MATCH (c)<-[:SUBCLASS_OF*0..]-(child:Class)
            OPTIONAL MATCH (o:Object {class_uri: child.uri})
            DETACH DELETE c, child, o
        """
        self.neo_repo.run_custom_query(query, {"uri": uri})


    def add_class_attribue(self, class_uri: str, title: str):
        """Добавить DatatypeProperty к классу."""
        prop_node = self.neo_repo.create_node(["DatatypeProperty"], {"title": title})
        self.neo_repo.create_arc(class_uri, prop_node["uri"], "HAS_DATATYPE_PROPERTY")
        return prop_node

    def delete_class_attribue(self, class_uri: str, title: str):
        query = """
            MATCH (c:Class {uri:$uri})-[:HAS_DATATYPE_PROPERTY]->(p:DatatypeProperty {title:$title})
            DETACH DELETE p
        """
        self.neo_repo.run_custom_query(query, {"uri": class_uri, "title": title})

    def add_class_object_attribute(self, class_uri: str, title: str, range_class_uri: str):
        """Добавить ObjectProperty узел + связь."""
        prop_node = self.neo_repo.create_node(["ObjectProperty"], {"title": title})
        # Привязываем property к классу и указываем range
        self.neo_repo.create_arc(class_uri, prop_node["uri"], "HAS_OBJECT_PROPERTY")
        self.neo_repo.create_arc(prop_node["uri"], range_class_uri, "RANGE")
        return prop_node

    def delete_class_object_attribute(self, object_property_uri: str):
        """Удалить ObjectProperty по uri узла."""
        query = """
            MATCH (p:ObjectProperty {uri:$uri})
            DETACH DELETE p
        """
        self.neo_repo.run_custom_query(query, {"uri": object_property_uri})

    def add_class_parent(self, parent_uri: str, target_uri: str):
        return self.neo_repo.create_arc(target_uri, parent_uri, "SUBCLASS_OF")

    def get_object(self, object_uri: str):
        return self.neo_repo.get_node_by_uri(object_uri)

    def delete_object(self, object_uri: str):
        return self.neo_repo.delete_node_by_uri(object_uri)

    def create_object(self, class_uri: str, title: str, description: str):
        obj = self.neo_repo.create_node(
            ["Object"],
            {"title": title, "description": description, "class_uri": class_uri}
        )
        self.neo_repo.create_arc(obj["uri"], class_uri, "INSTANCE_OF")
        return obj

    def update_object(self, object_uri: str, title: str = None, description: str = None):
        updates = {}
        if title: updates["title"] = title
        if description: updates["description"] = description
        return self.neo_repo.update_node(object_uri, updates)


    def collect_signature(self, class_uri: str):
        """Собрать все DatatypeProperty и ObjectProperty для класса."""
        query = """
            MATCH (c:Class {uri:$uri})
            OPTIONAL MATCH (c)-[:HAS_DATATYPE_PROPERTY]->(dp:DatatypeProperty)
            OPTIONAL MATCH (c)-[:HAS_OBJECT_PROPERTY]->(op:ObjectProperty)-[:RANGE]->(rc:Class)
            RETURN collect(distinct {title: dp.title, type:'datatype'}) as dprops,
                   collect(distinct {title: op.title, type:'object', range: rc.title, range_uri: rc.uri}) as oprops
        """
        record = self.neo_repo.run_custom_query(query, {"uri": class_uri})[0]
        return {
            "datatype_properties": [r for r in record["dprops"] if r["title"] is not None],
            "object_properties": [r for r in record["oprops"] if r["title"] is not None]
        }
