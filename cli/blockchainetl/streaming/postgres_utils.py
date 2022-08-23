from sqlalchemy.dialects.postgresql import insert


def create_insert_statement_for_table(table):
    insert_stmt = insert(table)

    primary_key_fields = [column.name for column in table.columns if column.primary_key]
    if primary_key_fields:
        insert_stmt = insert_stmt.on_conflict_do_nothing(
            index_elements=primary_key_fields,
        )

    return insert_stmt
