import unittest
from neo4j_repo import Neo4jRepository
from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
user = "neo4j"
password = "testpassword"

import unittest

class TestNeo4jOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Подключаемся к базе один раз для всех тестов
        cls.repo = Neo4jRepository(uri, user, password)
        if not cls.repo._driver:
            raise ConnectionError("Не удалось подключиться к Neo4j")

    @classmethod
    def tearDownClass(cls):
        # Закрываем подключение после завершения тестов
        cls.repo.close()

    def setUp(self):
    
        self.repo.run_custom_query("MATCH (n) DETACH DELETE n")

    def test_create_article_node(self):
        """Создать узел статьи."""
        article = self.repo.create_node(["Article"], {"title": "Test Article"})
        self.assertIsNotNone(article)
        self.assertIn("Article", article["labels"])
        self.assertEqual(article["properties"]["title"], "Test Article")

    def test_retrieve_all_articles(self):
        """Получить все статьи из базы."""
        self.repo.create_node(["Article"], {"title": "First"})
        self.repo.create_node(["Article"], {"title": "Second"})
        articles = self.repo.get_all_nodes()
        count_articles = sum(1 for node in articles if "Article" in node["labels"])
        self.assertEqual(count_articles, 2)

    def test_find_article_by_uri(self):
        """Найти статью по уникальному URI."""
        created = self.repo.create_node(["Article"], {"title": "Unique Article"})
        uri = created["uri"]
        fetched = self.repo.get_node_by_uri(uri)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched["properties"]["title"], "Unique Article")

    def test_search_articles_by_label(self):
        """Искать статьи по метке."""
        self.repo.create_node(["Article"], {"title": "Article 1"})
        self.repo.create_node(["Article"], {"title": "Article 2"})
        self.repo.create_node(["Blog"], {"title": "Blog Post"})
        articles = self.repo.get_nodes_by_labels(["Article"])
        titles = [node["properties"]["title"] for node in articles]
        self.assertIn("Article 1", titles)
        self.assertIn("Article 2", titles)
        self.assertNotIn("Blog Post", titles)

    def test_update_article_title(self):
        """Обновить название статьи."""
        article = self.repo.create_node(["Article"], {"title": "Old Title", "category": "tech"})
        uri = article["uri"]
        updated = self.repo.update_node(uri, {"title": "New Title"})
        self.assertEqual(updated["properties"]["title"], "New Title")
        self.assertEqual(updated["properties"]["category"], "tech")  # Поле осталось

    def test_delete_article_node(self):
        """Удалить узел статьи."""
        article = self.repo.create_node(["Article"], {"title": "To delete"})
        uri = article["uri"]
        result = self.repo.delete_node_by_uri(uri)
        self.assertTrue(result)
        self.assertIsNone(self.repo.get_node_by_uri(uri))

    def test_create_authorship_relationship(self):
        """Создать отношение авторства между пользователем и статьей."""
        author = self.repo.create_node(["User"], {"name": "John"})
        article = self.repo.create_node(["Article"], {"title": "Sample"})
        rel = self.repo.create_arc(author["uri"], article["uri"], "AUTHORED")
        self.assertIsNotNone(rel)
        self.assertEqual(rel["uri"], "AUTHORED")

    def test_get_users_with_articles(self):
        """Получить пользователей с их статьями."""
        user1 = self.repo.create_node(["User"], {"name": "Alice"})
        article1 = self.repo.create_node(["Article"], {"title": "Post 1"})
        self.repo.create_arc(user1["uri"], article1["uri"], "AUTHORED")
        user2 = self.repo.create_node(["User"], {"name": "Bob"})
        article2 = self.repo.create_node(["Article"], {"title": "Post 2"})
        self.repo.create_arc(user2["uri"], article2["uri"], "AUTHORED")

        users = self.repo.get_all_nodes_and_arcs()
        alice_data = next((u for u in users if u["properties"]["name"] == "Alice"), None)
        print("Alice arcs:", alice_data.get("arcs"))
        self.assertIsNotNone(alice_data)
        self.assertEqual(len(alice_data["arcs"]), 1)
        self.assertEqual(alice_data["arcs"][0]["types"], "AUTHORED")

    def test_delete_authorship_relationship(self):
        """Удалить отношение авторства."""
        author = self.repo.create_node(["User"], {"name": "Charlie"})
        article = self.repo.create_node(["Article"], {"title": "Remove me"})
        rel = self.repo.create_arc(author["uri"], article["uri"], "AUTHORED")
        result = self.repo.delete_arc_by_id(rel["id"])
        self.assertTrue(result)
        author_data = self.repo.get_node_by_uri(author["uri"])
        self.assertEqual(len(author_data["arcs"]), 0)

if __name__ == '__main__':
    unittest.main()