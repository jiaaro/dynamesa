import os
import unittest
import subprocess
import dynamesa
from dynamesa import Key, Attr, PRIMARY_KEY, REMOVE_KEY


class DynamoTests(unittest.TestCase):
    def setUp(self) -> None:
        dynamesa.configure(
            region_name="localhost",
            endpoint_url=os.environ.get("DYNAMO_ENDPOINT", "http://127.0.0.1:2808"),
            aws_access_key_id="AKLOCAL",
            aws_secret_access_key="SKLOCAL",
        )
        try:
            subprocess.call(
                ["docker", "run", "-d", "--name=dynamesa-test-db", "-p", "2808:8000", "amazon/dynamodb-local"],
                stderr=os.devnull,
            )
        except:
            pass
        try:
            dynamesa.tables.User.table.delete()
        except:
            pass
        self.User = dynamesa.tables.create(
            "User",
            ("id", "S", "ts", "N"),
            gsis={"AgeIndex": ("age", "N")},
        )

    def tearDown(self):
        dynamesa.tables.User.table.delete()

    def assertFindResults(self, n, *findargs, **findkwargs):
        self.assertEqual(len(list(self.User.find(*findargs, **findkwargs))), n)

    def test_dynamesa(self):
        User = self.User

        uid, uts = "1", 1600000000
        User.put({"id": uid, "ts": uts, "name": "Jack Frost", "hair": "white", "nickname": "Jackie"})
        User.put({"id": "2", "ts": 1700000000, "name": "Frosty", "hair": "none"})
        User.put({"id": "3", "ts": 1500000000, "name": "Santa", "hair": "white"})

        self.assertFindResults(3)

        self.assertEqual(
            User.get(id=uid, ts=uts)["nickname"],
            "Jackie",
        )

        User.update(
            {
                "id": uid,
                "ts": uts,
                "age": 823,
                "email": "jf@northpole.net",
                "nickname": REMOVE_KEY,
            }
        )
        jf = User.get(id=uid, ts=uts)
        self.assertEqual(jf["name"], "Jack Frost")
        self.assertNotIn("nickname", jf)

        self.assertEqual(
            User.get(id=uid, ts=uts)["name"],
            "Jack Frost",
        )

        with self.assertRaises(User.DoesNotExist):
            User.get(id="7", ts=1600000000)

        self.assertFindResults(1, PRIMARY_KEY, id=uid, ts=uts)
        self.assertFindResults(1, PRIMARY_KEY, Key("id").eq(uid) & Key("ts").gt(500000))
        self.assertFindResults(2, hair="white")
        self.assertFindResults(2, Key("hair").eq("white"))
        self.assertFindResults(1, "AgeIndex", age=823)
        self.assertFindResults(0, "AgeIndex", age=123)

        hairless_users = list(User.find(hair="none"))
        self.assertEqual(len(hairless_users), 1)
        self.assertEqual(hairless_users[0]["name"], "Frosty")

        # Delete all white-haired users
        User.clear(hair="white")
        self.assertFindResults(1)

        # Wipe table
        User.clear()
        self.assertFindResults(0)
