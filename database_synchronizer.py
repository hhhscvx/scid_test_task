import sqlite3
from sqlite3 import Connection

from logger import logger


class DatabaseSynchronizer:
    def __init__(
        self,
        test_db_path: str,
        main_db_path: str,
    ) -> None:
        """
        Инициализация синхронизатора с путями к базам данных.
        """
        self.test_db_path = test_db_path
        self.main_db_path = main_db_path

    def connect(self, db_path: str) -> Connection:
        """Подключение БД"""
        logger.info(f"Подключаемся к БД '{db_path}'")
        return sqlite3.connect(db_path)

    def fetch_all_records(
        self,
        connection: Connection,
        table_name: str,
    ) -> tuple[list, list[str]]:
        """Извлечение всех записей из таблицы"""
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        records = cursor.fetchall()
        logger.warning(f"Извлекали данные из таблицы '{table_name}': {records}")
        columns = [desc[0] for desc in cursor.description]
        logger.warning(f"Извлекали данные из таблицы '{table_name}': {columns}")
        return records, columns

    def synchronize_table(
        self,
        table_name: str,
        primary_key: str,
    ) -> None:
        """
        Синхронизация таблицы между базами данных.
        """
        source_conn = self.connect(self.test_db_path)
        target_conn = self.connect(self.main_db_path)

        try:
            source_records, columns = self.fetch_all_records(connection=source_conn,
                                                         table_name=table_name)
            target_records, _ = self.fetch_all_records(connection=target_conn,
                                                   table_name=table_name)

            source_dict = {record[columns.index(primary_key)]: record for record in source_records}
            target_dict = {record[columns.index(primary_key)]: record for record in target_records}

            logger.info(f"source_dict: {source_dict}")
            logger.info(f"target_dict: {target_dict}")

            cursor = target_conn.cursor()
            for key, source_record in source_dict.items():
                if key in target_dict:
                    update_values = [f"{col} = ?" for col in columns if col != primary_key]
                    query = f"UPDATE {table_name} SET {', '.join(update_values)} WHERE {primary_key} = ?"
                    cursor.execute(query, [*source_record[1:], key])
                else:
                    placeholders = ", ".join(["?" for _ in columns])
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(query, source_record)

            target_conn.commit()
        finally:
            source_conn.close()
            target_conn.close()

    def synchronize(self, table_config: dict[str, str]) -> None:
        """
        Синхронизация всех указанных таблиц.
        """
        for table_name, primary_key in table_config.items():
            self.synchronize_table(table_name, primary_key)


def main():
    source_db = "source.db"
    target_db = "target.db"
    table_config = {
        "users": "id",
        "orders": "id"
    }

    synchronizer = DatabaseSynchronizer(test_db_path=source_db,
                                        main_db_path=target_db)
    synchronizer.synchronize(table_config)


if __name__ == "__main__":
    main()
