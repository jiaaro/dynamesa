import functools
import operator
from itertools import zip_longest
from typing import Iterator

import boto3

# for easy access
from boto3.dynamodb.conditions import Key, Attr

# sentinal values
PRIMARY_KEY = object()
REMOVE_KEY = object()


class DoesNotExist(Exception):
    pass


class Table:
    def __init__(self, table_name, **kwargs):
        dynamodb = boto3.resource("dynamodb", **kwargs)
        self.table = dynamodb.Table(table_name)
        self.DoesNotExist = type(f"DoesNotExist", (DoesNotExist,), {})

    def __repr__(self):
        return f"<Table: {self.table.name}>"

    def __str__(self):
        return f"{self.table.name} ({self.table.creation_date_time:%Y-%m-%d}, {self.table.item_count} items)"

    def get(self, **kwargs):
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
        return item

    def put(self, item):
        self.table.put_item(Item=item)
        return item

    def update(self, update, return_values="ALL_NEW"):
        """
        Takes a table and a dictionary of updates, extracts the primary key from the
        update dict and applies the remaining keys as an update to the record.

        Pass return_values="NONE" if you don't care what the resulting record is.
        """
        table = self.table
        update = update.copy()
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
            if val is REMOVE_KEY:
                remove_parts.append(f"#a{i}")
            else:
                expression_vals[f":v{i}"] = val
                set_parts.append(f"#a{i} = :v{i}")

        update_expression = ""
        if set_parts:
            update_expression += "SET " + ", ".join(set_parts)
        if remove_parts:
            update_expression += " REMOVE" + ", ".join(remove_parts)

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
        return res.get("Attributes")

    def find(self, *args, **kwargs):
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
                yield item

    def clear(self, *args, **kwargs):
        with self.table.batch_writer() as batch:
            for item in self.find(*args, **kwargs):
                batch.delete_item(Key={k["AttributeName"]: item[k["AttributeName"]] for k in self.table.key_schema})


class _TableGetter:
    _resource_kwargs = {}
    _tables = {}

    def configure(self, **kwargs):
        self._resource_kwargs.update(kwargs)

    @property
    def dynamodb(self):
        return boto3.resource("dynamodb", **self._resource_kwargs)

    def reload(self):
        res = self.dynamodb.meta.client.list_tables()
        self._tables = {}
        for tablename in res["TableNames"]:
            self._tables[tablename] = Table(tablename, **self._resource_kwargs)

    def create(self, table_name, pk, gsis={}, lsis={}) -> Table:
        attribute_types = {}

        def parse_index(idx):
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
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in attribute_types.items()],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
            **create_kwargs,
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        return self[table_name]

    def __getattr__(self, item) -> Table:
        return self[item]

    def __getitem__(self, item) -> Table:
        if item not in self._tables:
            self.reload()
        return self._tables[item]

    def __iter__(self) -> Iterator[Table]:
        if not self._tables:
            self.reload()
        return iter(self._tables.values())

    def __repr__(self):
        max_table_name_len = max(len(t.table.name) for t in self)
        return "Dynamesa Tables:\n" + "\n".join(
            f"  {t.table.name.ljust(max_table_name_len)}  ({t.table.creation_date_time:%Y-%m-%d}, {t.table.item_count} items)"
            for t in self
        )


tables = _TableGetter()
configure = tables.configure
