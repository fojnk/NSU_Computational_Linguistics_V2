import unittest
from neo4j_driver.neo4j_repo import Neo4jRepository
from ontology_repo import OntologyRepository

uri = "bolt://localhost:7687"
user = "neo4j"
password = "testpassword"


class TestOntologyRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.neo = Neo4jRepository(uri, user, password)
        cls.repo = OntologyRepository(cls.neo)
        cls.neo.run_custom_query("MATCH (n) DETACH DELETE n")

    @classmethod
    def tearDownClass(cls):
        cls.neo.close()

    def test_class_crud(self):
        # Создание класса
        c1 = self.repo.create_class("Person", "A human being")
        self.assertIn("uri", c1)

        # Получение класса
        got = self.repo.get_class(c1["uri"])
        self.assertEqual(got["properties"]["title"], "Person")

        # Обновление класса
        updated = self.repo.update_class(c1["uri"], description="Updated desc")
        self.assertEqual(updated["properties"]["description"], "Updated desc")

        # Создание дочернего класса
        c2 = self.repo.create_class("Student", "Learner", parent_uri=c1["uri"])
        children = self.repo.get_class_children(c1["uri"])
        self.assertTrue(any(ch["properties"]["title"] == "Student" for ch in children))

        # Проверка родителей
        parents = self.repo.get_class_parents(c2["uri"])
        self.assertTrue(any(p["properties"]["title"] == "Person" for p in parents))

        # Удаление класса
        self.repo.delete_class(c1["uri"])
        self.assertIsNone(self.repo.get_class(c1["uri"]))

    def test_attributes_and_signature(self):
        # Создаем класс Book
        cls_node = self.repo.create_class("Book", "Readable item")

        # Добавляем DatatypeProperty
        dp = self.repo.add_class_attribute(cls_node["uri"], "title")
        self.assertEqual(dp["properties"]["title"], "title")

        # Создаем связанный класс Author
        c2 = self.repo.create_class("Author", "Writer of books")

        # Добавляем ObjectProperty
        op = self.repo.add_class_object_attribute(cls_node["uri"], "WRITTEN_BY", c2["uri"])
        self.assertEqual(op["properties"]["title"], "WRITTEN_BY")

        # Collect signature
        sig = self.repo.collect_signature(cls_node["uri"])
        self.assertTrue(any(f["title"] == "title" for f in sig["datatype_properties"]))
        self.assertTrue(any(f["title"] == "WRITTEN_BY" for f in sig["object_properties"]))

        # Удаление DatatypeProperty
        self.repo.delete_class_attribute(cls_node["uri"], "title")
        sig2 = self.repo.collect_signature(cls_node["uri"])
        self.assertFalse(any(f["title"] == "title" for f in sig2["datatype_properties"]))

    def test_object_crud_with_properties(self):
        # Создаем класс Car
        car_cls = self.repo.create_class("Car", "Vehicle")
        self.repo.add_class_attribute(car_cls["uri"], "model")
        self.repo.add_class_attribute(car_cls["uri"], "year")

        # Создаем объект с полями
        obj = self.repo.create_object(
            car_cls["uri"],
            properties={"title": "Tesla", "description": "Electric", "model": "S", "year": 2022}
        )
        self.assertIn("uri", obj)

        # Проверяем базовые свойства
        got = self.repo.get_object(obj["uri"])
        self.assertEqual(got["properties"]["title"], "Tesla")

        # Collect signature проверяет наличие полей
        sig = self.repo.collect_signature(car_cls["uri"])
        self.assertTrue(any(f["title"] == "model" for f in sig["datatype_properties"]))
        self.assertTrue(any(f["title"] == "year" for f in sig["datatype_properties"]))

        # Обновление объекта
        updated = self.repo.update_object(obj["uri"], {"description": "Updated"})
        self.assertEqual(updated["properties"]["description"], "Updated")

        # Удаление объекта
        self.repo.delete_object(obj["uri"])
        self.assertIsNone(self.repo.get_object(obj["uri"]))

    def test_object_with_relations(self):
        # Создаем класс City
        city_cls = self.repo.create_class("City", "Place where people live")
        self.repo.add_class_attribute(city_cls["uri"], "name")

        # Создаем класс Person
        person_cls = self.repo.create_class("Person", "A human")
        self.repo.add_class_object_attribute(person_cls["uri"], "LIVES_IN", city_cls["uri"])

        # Создаем объект City
        moscow = self.repo.create_object(city_cls["uri"], {"title": "Moscow", "name": "Moscow"})

        # Создаем объект Person с relation -> City
        alex = self.repo.create_object(
            person_cls["uri"],
            properties={"title": "Alex"},
            relations={"LIVES_IN": moscow["uri"]}
        )
        self.assertIn("uri", alex)

        # Проверка объекта
        got = self.repo.get_object(alex["uri"])
        self.assertEqual(got["properties"]["title"], "Alex")


if __name__ == "__main__":
    unittest.main()
