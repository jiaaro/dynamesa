import functools
import operator

import boto3

# for easy access
from boto3.dynamodb.conditions import Key, Attr

# sentinal value
REMOVE_KEY = object()


class Table:
    def __init__(self, table_name, **kwargs):
        dynamodb = boto3.resource("dynamodb", **kwargs)
        self.table = dynamodb.Table(table_name)

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

    def find(self, dynamo_table_index_name_=None, **kwargs):
        conditions = None
        if kwargs:
            conditions = functools.reduce(operator.and_, [Key(k).eq(v) for k, v in kwargs.items()])

        paginate_kwargs = {}
        if dynamo_table_index_name_:
            paginate_kwargs["IndexName"] = dynamo_table_index_name_
            if conditions:
                paginate_kwargs["KeyConditionExpression"] = conditions
        elif conditions:
            paginate_kwargs["FilterExpression"] = conditions

        client = self.table.meta.client
        if "IndexName" in paginate_kwargs:
            paginator = client.get_paginator("query").paginate(TableName=self.table.name, **paginate_kwargs)
        else:
            paginator = client.get_paginator("scan").paginate(TableName=self.table.name, **paginate_kwargs)

        for page in paginator:
            for item in page["Items"]:
                yield item

    def clear(self, **paginate_kwargs):
        client = self.table.meta.client
        if "IndexName" in paginate_kwargs:
            paginator = client.get_paginator("query").paginate(TableName=self.table.name, **paginate_kwargs)
        else:
            paginator = client.get_paginator("scan").paginate(TableName=self.table.name, **paginate_kwargs)

        with self.table.batch_writer() as batch:
            for page in paginator:
                for item in page["Items"]:
                    batch.delete_item(Key={k["AttributeName"]: item[k["AttributeName"]] for k in self.table.key_schema})


class _TableGetter:
    _resource_kwargs = {}
    _tables = {}

    def configure(self, **kwargs):
        self._resource_kwargs.update(kwargs)

    def __getattr__(self, item):
        if item not in self._tables:
            dynamodb = boto3.resource("dynamodb", **self._resource_kwargs)
            res = dynamodb.meta.client.list_tables()
            for tablename in res["TableNames"]:
                self._tables[tablename] = Table(tablename, **self._resource_kwargs)
        return self._tables[item]


tables = _TableGetter()
configure = tables.configure
