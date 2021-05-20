from data_warehouse import *


for key in rel2schema:
    schema = rel2schema[key]["schema"]
    path = rel2schema[key]["df_path"]

    df = get_or_create_dataframe(schema, path)
    df.show()
