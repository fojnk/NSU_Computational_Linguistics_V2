import unittest
from neo4j_driver.neo4j_repo import Neo4jRepository
from ontology_repo import OntologyRepository

uri = "bolt://localhost:7687"
user = "neo4j"
password = "testpassword"


class TestOntologyRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Подключение к базе (замени логин/пароль при необходимости)
        cls.neo = Neo4jRepository(uri, user, password)
        cls.repo = OntologyRepository(cls.neo)

        # Очистим базу перед тестами
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

    def test_object_crud(self):
        # Создание класса
        cls_node = self.repo.create_class("Car", "Vehicle")

        # Создание объекта
        obj = self.repo.create_object(cls_node["uri"], "My Tesla", "Electric car")
        self.assertIn("uri", obj)

        # Получение объекта
        got = self.repo.get_object(obj["uri"])
        self.assertEqual(got["properties"]["title"], "My Tesla")

        # Обновление объекта
        updated = self.repo.update_object(obj["uri"], description="Updated description")
        self.assertEqual(updated["properties"]["description"], "Updated description")

        # Удаление объекта
        self.repo.delete_object(obj["uri"])
        self.assertIsNone(self.repo.get_object(obj["uri"]))

    def test_attributes_and_signature(self):
        # Создаем класс
        cls_node = self.repo.create_class("Book", "Readable item")

        # Добавляем DatatypeProperty
        dp = self.repo.add_class_attribue(cls_node["uri"], "title")
        self.assertEqual(dp["properties"]["title"], "title")

        # Создаем связанный класс
        c2 = self.repo.create_class("Author", "Writer of books")

        # Добавляем ObjectProperty
        op = self.repo.add_class_object_attribute(cls_node["uri"], "WRITTEN_BY", c2["uri"])
        self.assertEqual(op["properties"]["title"], "WRITTEN_BY")

        # Collect signature
        sig = self.repo.collect_signature(cls_node["uri"])
        self.assertTrue(any(f["title"] == "title" for f in sig["datatype_properties"]))
        self.assertTrue(any(f["title"] == "WRITTEN_BY" for f in sig["object_properties"]))

        # Удаление DatatypeProperty
        self.repo.delete_class_attribue(cls_node["uri"], "title")
        sig2 = self.repo.collect_signature(cls_node["uri"])
        self.assertFalse(any(f["title"] == "title" for f in sig2["datatype_properties"]))


if __name__ == "__main__":
    unittest.main()
