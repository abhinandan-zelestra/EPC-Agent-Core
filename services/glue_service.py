import boto3

glue = boto3.client("glue")

def get_glue_schema(database_name: str):

    response = glue.get_tables(DatabaseName=database_name)

    schema_text = ""

    for table in response["TableList"]:
        table_name = table["Name"]
        schema_text += f"\nTable: {table_name}\nColumns:\n"

        for column in table["StorageDescriptor"]["Columns"]:
            schema_text += f"- {column['Name']} ({column['Type']})\n"

    return schema_text