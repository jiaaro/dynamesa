import json
import os
import unittest
import subprocess
import typing as ty
from dataclasses import dataclass

import dynamesa
from dynamesa import Key, Attr, PRIMARY_KEY, REMOVE_KEY, MISSING_KEY


@dataclass
class UserModel:
    id: str
    ts: int
    name: str
    age: ty.Optional[int] = MISSING_KEY
    email: str = ""
    nickname: str = ""
    hair: str = ""


class DynamoTests(unittest.TestCase):
    def setUp(self) -> None:
        dynamesa.configure(
            region_name="localhost",
            endpoint_url=os.environ.get("DYNAMO_ENDPOINT", "http://127.0.0.1:2808"),
            aws_access_key_id="AKLOCAL",
            aws_secret_access_key="SKLOCAL",
        )
        container_name = "dynamesa-test-db"
        inspect_container = ["docker", "container", "inspect", container_name]
        delete_container = ["docker", "container", "rm", container_name]
        run_container = ["docker", "run", "-d", "--name", container_name, "-p2808:8000", "amazon/dynamodb-local"]
        try:
            result = subprocess.check_output(inspect_container, stderr=subprocess.DEVNULL)
            container_info = json.loads(result)[0]
            if not container_info["State"]["Running"]:
                subprocess.call(delete_container, stderr=subprocess.DEVNULL)
                subprocess.call(run_container, stderr=subprocess.DEVNULL)
        except:
            subprocess.call(run_container, stderr=subprocess.DEVNULL)

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

    def test_dynamesa_with_typing(self):
        User = dynamesa.tables.get("User", item_type=UserModel)

        uid, uts = "1", 1600000000
        User.put({"id": uid, "ts": uts, "name": "Jack Frost", "hair": "white", "nickname": "Jackie"})
        User.put({"id": "2", "ts": 1700000000, "name": "Frosty", "hair": "none"})
        User.put({"id": "3", "ts": 1500000000, "name": "Santa", "hair": "white"})

        self.assertFindResults(3)

        jack = User.get(id=uid, ts=uts)
        self.assertEqual(
            jack.nickname,
            "Jackie",
        )
        jack.nickname = REMOVE_KEY
        User.update(jack)

        jf = User.get(id=uid, ts=uts)
        self.assertEqual(jf.name, "Jack Frost")
        self.assertEqual(jf.nickname, "")
        User.update(
            dict(
                id=uid,
                ts=uts,
                email=REMOVE_KEY,
            )
        )
        jf = User.get(id=uid, ts=uts)
        self.assertEqual(jf.name, "Jack Frost")
        self.assertEqual(jf.email, "")
        jf.age = 823
        User.update(jf)

        self.assertEqual(
            User.get(id=uid, ts=uts).name,
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
        self.assertEqual(hairless_users[0].name, "Frosty")

        # Delete all white-haired users
        User.clear(hair="white")
        self.assertFindResults(1)

        # Wipe table
        User.clear()
        self.assertFindResults(0)
