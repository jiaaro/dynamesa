import dataclasses
import functools
import operator
import unittest
from itertools import zip_longest
import typing as ty

import boto3  # type: ignore

# for easy access
from boto3.dynamodb.conditions import Key, Attr  # type: ignore

# sentinal values
from botocore.exceptions import ClientError


class Sentinal:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"<Sentinal: {self.name!r}>"

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        memodict[id(self)] = self
        return self


PRIMARY_KEY = Sentinal("Primary Key")
REMOVE_KEY = Sentinal("Remove Key")
MISSING_KEY = Sentinal("Missing Key")


class DoesNotExist(Exception):
    pass


def itemdict(item) -> ty.Dict:
    if isinstance(item, dict):
        d = item
    elif hasattr(item, "asdict"):
        d = item.asdict()
    else:
        d = dataclasses.asdict(item)
    return {k: v for (k, v) in d.items() if v is not MISSING_KEY}


T = ty.TypeVar("T", ty.Dict[str, ty.Any], ty.Any)


class Table(ty.Generic[T]):
    def __init__(self, table_name: str, item_type: ty.Type[T] = dict, **kwargs):
        dynamodb = boto3.resource("dynamodb", **kwargs)
        self.item_type: ty.Type[T] = item_type
        self.table = dynamodb.Table(table_name)
        self.DoesNotExist = type(f"DoesNotExist", (DoesNotExist,), {})

    def __repr__(self):
        return f"<Table: {self.table.name}>"

    def __str__(self):
        return f"{self.table.name} ({self.table.creation_date_time:%Y-%m-%d}, {self.table.item_count} items)"

    def get(self, **kwargs) -> T:
        dynamo_key = {}
        for k in self.table.key_schema:
            if k["AttributeName"] not in kwargs:
                raise ValueError(
                    f"table.get was missing {k['KeyType']} key, {k['AttributeName']} for table {self.table.name}"
                )
            else:
                dynamo_key[k["AttributeName"]] = kwargs[k["AttributeName"]]

        unexpected_kwargs = set(kwargs.keys()) - set(dynamo_key.keys())
        if unexpected_kwargs:
            raise ValueError(f"table.get recieved unexpected keyword arguments: {unexpected_kwargs!r}")
        item = self.table.get_item(Key=dynamo_key).get("Item")
        if not item:
            raise self.DoesNotExist(dynamo_key)
        return self.item_type(**item)

    def put(self, item: T) -> T:
        self.table.put_item(Item=itemdict(item))
        return item

    def update(self, update: dict, return_values: str = "ALL_NEW") -> ty.Union[T, dict, None]:
        """
        Takes a table and a dictionary of updates, extracts the primary key from the
        update dict and applies the remaining keys as an update to the record.

        Pass return_values="NONE" if you don't care what the resulting record is.
        """
        table = self.table
        orig_update = update
        update = itemdict(update).copy()
        pk = {}
        for k in table.key_schema:
            key = k["AttributeName"]
            if key not in update:
                raise ValueError(
                    f"Couldn't update {table.table_name} because update dict is missing the {k['KeyType']} key, {key:!r}"
                )
            pk[key] = update.pop(key)

        if not update:
            raise ValueError("There were no updates to apply, update dict contained only the primary key")

        expression_attrs = {}
        expression_vals = {}
        set_parts = []
        remove_parts = []
        for i, (key, val) in enumerate(update.items()):
            expression_attrs[f"#a{i}"] = key
            if val is MISSING_KEY:
                continue
            elif val is REMOVE_KEY:
                if isinstance(orig_update, dict):
                    orig_update.pop(key)
                else:
                    setattr(orig_update, key, MISSING_KEY)
                remove_parts.append(f"#a{i}")
            else:
                expression_vals[f":v{i}"] = val
                set_parts.append(f"#a{i} = :v{i}")

        update_expression = ""
        if set_parts:
            update_expression += "SET " + ", ".join(set_parts)
        if remove_parts:
            update_expression += " REMOVE " + ", ".join(remove_parts)

        kwargs = {}
        if expression_attrs:
            kwargs["ExpressionAttributeNames"] = expression_attrs
        if expression_vals:
            kwargs["ExpressionAttributeValues"] = expression_vals

        res = table.update_item(
            Key=pk,
            ReturnValues=return_values,
            UpdateExpression=update_expression,
            **kwargs,
        )
        item = res.get("Attributes")
        if return_values == "ALL_NEW":
            item = self.item_type(**item)
        return item

    def find(self, *args, **kwargs) -> ty.Generator[T, None, None]:
        # if the first arg is a string, it's the name of the index to use
        index_name = None
        if args and (args[0] is PRIMARY_KEY or isinstance(args[0], str)):
            index_name = args[0]
            args = args[1:]

        paginate_kwargs = {}
        if index_name:
            if index_name is not PRIMARY_KEY:
                paginate_kwargs["IndexName"] = index_name
            # When there is a positional arg after the index name, it's a key condition expression
            if args and args[0]:
                paginate_kwargs["KeyConditionExpression"] = args[0]
                args = args[1:]
            else:
                if index_name is PRIMARY_KEY:
                    idx_key_schema = self.table.key_schema
                else:
                    idx = next(idx for idx in self.table.global_secondary_indexes if idx["IndexName"] == index_name)
                    idx_key_schema = idx["KeySchema"]
                idx_keys = {a["AttributeName"] for a in idx_key_schema}
                paginate_kwargs["KeyConditionExpression"] = functools.reduce(
                    operator.and_, [Key(k).eq(kwargs[k]) for k in idx_keys]
                )
            filters = [Key(k).eq(v) for k, v in kwargs.items() if k not in idx_keys]
            if args:
                assert (
                    len(args) == 1
                ), "table.find takes at most 3 positional arguments: index name, key condition expression, and filter expression"
                filters.append(args[0])
            if filters:
                paginate_kwargs["FilterExpression"] = functools.reduce(operator.and_, filters)
        elif args or kwargs:
            filters = [Key(k).eq(v) for k, v in kwargs.items()]
            if args:
                assert len(args) == 1
                filters.append(args[0])
            paginate_kwargs["FilterExpression"] = functools.reduce(operator.and_, filters)

        client = self.table.meta.client
        if index_name:
            paginator = client.get_paginator("query").paginate(TableName=self.table.name, **paginate_kwargs)
        else:
            paginator = client.get_paginator("scan").paginate(TableName=self.table.name, **paginate_kwargs)

        for page in paginator:
            for item in page["Items"]:
                yield self.item_type(**item)

    def clear(self, *args, **kwargs) -> None:
        with self.table.batch_writer() as batch:
            for item in self.find(*args, **kwargs):
                item = itemdict(item)
                batch.delete_item(Key={k["AttributeName"]: item[k["AttributeName"]] for k in self.table.key_schema})


# Either (Hash key, type) or (hash key, hashkey type, range key, range key type)
DynamesaIndexType = ty.Union[ty.Tuple[str, str], ty.Tuple[str, str, str, str]]


class _TableGetter:
    _resource_kwargs: ty.Dict[str, ty.Any] = {}
    _tables: ty.Dict[ty.Tuple[str, ty.Type], Table] = {}
    table_name_prefix: str = ""

    def configure(self, **kwargs) -> None:
        self._resource_kwargs.update(kwargs)

    @property
    def dynamodb(self):
        return boto3.resource("dynamodb", **self._resource_kwargs)

    def reload(self) -> None:
        res = self.dynamodb.meta.client.list_tables()
        self._tables = {}
        for tablename in res["TableNames"]:
            self._tables[tablename, dict] = Table(tablename, **self._resource_kwargs)

    def create(
        self,
        table_name: str,
        pk: DynamesaIndexType,
        gsis: ty.Dict[str, DynamesaIndexType] = {},
        lsis: ty.Dict[str, DynamesaIndexType] = {},
        item_type: ty.Type[T] = dict,
    ) -> Table[T]:
        prefixed_table_name = f"{self.table_name_prefix}{table_name}"
        attribute_types = {}

        def parse_index(idx: DynamesaIndexType) -> ty.List[ty.Dict[str, ty.Any]]:
            assert len(idx) % 2 == 0
            key_types = ("HASH", "RANGE")
            index = []
            for i, (k, dynamotype) in enumerate(zip_longest(*[iter(idx)] * 2)):
                attribute_types[k] = dynamotype
                index.append({"AttributeName": k, "KeyType": key_types[i]})
            return index

        create_kwargs = {}
        create_kwargs["KeySchema"] = parse_index(pk)

        for idx_name, idx_def in gsis.items():
            create_kwargs.setdefault("GlobalSecondaryIndexes", [])
            create_kwargs["GlobalSecondaryIndexes"].append(
                {
                    "IndexName": idx_name,
                    "KeySchema": parse_index(idx_def),
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
                }
            )

        for idx_name, idx_def in lsis.items():
            create_kwargs.setdefault("LocalSecondaryIndexes", [])
            create_kwargs["LocalSecondaryIndexes"].append(
                {
                    "IndexName": idx_name,
                    "KeySchema": parse_index(idx_def),
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
                }
            )

        table = self.dynamodb.create_table(
            TableName=prefixed_table_name,
            AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in attribute_types.items()],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
            **create_kwargs,
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=prefixed_table_name)
        return self.get(table_name, item_type)

    def delete(self, table_name):
        if isinstance(table_name, Table):
            prefixed_table_name = table_name.table.name
        else:
            prefixed_table_name = f"{self.table_name_prefix}{table_name}"

        self.dynamodb.meta.client.delete_table(TableName=prefixed_table_name)
        self.dynamodb.meta.client.get_waiter("table_not_exists").wait(TableName=prefixed_table_name)
        for table_key in list(self._tables.keys()):
            if table_key[0] == prefixed_table_name:
                self._tables.pop(table_key)

    def get(self, table_name: str, item_type: ty.Type[T] = dict) -> Table[T]:
        table_name = f"{self.table_name_prefix}{table_name}"
        if (table_name, dict) not in self._tables:
            self.reload()
        if (table_name, item_type) in self._tables:
            return self._tables[table_name, item_type]

        table = Table(table_name, item_type=item_type, **self._resource_kwargs)
        self._tables[table_name, item_type] = table
        return table

    def __getattr__(self, table_name) -> Table:
        if not table_name.startswith("__"):
            return self.get(table_name)

    def __getitem__(self, table_name) -> Table:
        return self.get(table_name)

    def __iter__(self) -> ty.Iterator[Table]:
        if not self._tables:
            self.reload()
        return iter(t for t in self._tables.values() if t.table.name.startswith(self.table_name_prefix))

    def __len__(self):
        if not self._tables:
            self.reload()
        return len(self._tables)

    def __repr__(self):
        max_table_name_len = max(len(t.table.name) for t in self)
        return "Dynamesa Tables:\n" + "\n".join(
            f"  {t.table.name.ljust(max_table_name_len)}  ({t.table.creation_date_time:%Y-%m-%d}, {t.table.item_count} items)"
            for t in self
        )


tables = _TableGetter()
configure = tables.configure


class DynamoUnitTestMixin(unittest.TestCase):
    dynamesa_table_name_prefix: str = ""
    dynamesa_tables = []

    @classmethod
    def setUpClass(cls) -> None:
        global tables, configure

        if hasattr(cls, "dynamesa_configure"):
            cls._old_table_getter = tables
            cls.tables = _TableGetter()
            cls.tables.configure(**cls.dynamesa_configure)
            tables = cls.tables
            configure = tables.configure
        else:
            cls._old_table_getter = tables
            cls.tables = tables

        if not hasattr(cls, "_old_dynamesa_table_name_prefix"):
            cls._old_dynamesa_table_name_prefix = tables.table_name_prefix

        tables.table_name_prefix = cls.dynamesa_table_name_prefix
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        global tables, configure
        super().tearDownClass()
        tables.dynamesa_table_name_prefix = cls._old_dynamesa_table_name_prefix
        tables = cls._old_table_getter
        configure = tables.configure

    def setUp(self) -> None:
        should_replace = None

        def mktable(table):
            if isinstance(table, (list, tuple)):
                tables.create(*table)
            elif isinstance(table, dict):
                tables.create(**table)

        for table in self.dynamesa_tables:
            try:
                mktable(table)
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceInUseException":
                    raise

                if should_replace is None:
                    replace_answer = input(
                        "Couldn't create tables. Would you like to delete existing tables and replace them? [yN]"
                    )
                    should_replace = replace_answer.lower() == "y"
                if not should_replace:
                    raise

                try:
                    table_name = table[0]
                except (KeyError, IndexError, TypeError):
                    table_name = table.get("table_name")
                tables.delete(table_name)
                mktable(table)

        if should_replace:
            tables.reload()

        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()
        for table in tables:
            table.table.delete()
