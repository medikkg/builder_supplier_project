import pandas as pd
import psycopg2
from config import host, password, port, db_name, user
from prettytable import *
import datetime

table_name = 'materials'
fileName = 'katalog.xlsx'
TXTFile = r'C:\Users\User\PycharmProjects\pythonProject1\Faker_output.txt'


class Materials:
    """PostgresQL database class"""

    def __init__(self):
        self.connection = psycopg2.connect(user=user,
                                           password=password,
                                           host=host,
                                           port=port,
                                           database=db_name)
        self.cursor = self.connection.cursor()
        self.connection.autocommit = True

    def create_main_table(self):
        """
        Метод создания основной таблицы материалов
        :return:
        """
        try:
            with self.connection.cursor() as cursor:
                createTableQuery = f"CREATE TABLE IF NOT EXISTS %s (" \
                                   f"id serial PRIMARY KEY NOT NULL," \
                                   f"name text," \
                                   f"units text," \
                                   f"price int," \
                                   f"total int NOT NULL," \
                                   f"balance int NOT NULL," \
                                   f"orders_qnt int NOT NULL," \
                                   f"company text," \
                                   f"purpose text);"
                cursor.execute(createTableQuery % table_name)

                self.create_table_supply_materials()  # таблица заказанных материалов автоматически создается
                self.create_table_delivered_materials()  # таблица доставленных материалов автоматически создается
                self.create_table_districts()  # таблица районов с данными автоматически создается
                self.create_table_build_objects()  # таблица строящихся объектов автоматически создается
                self.create_table_finished_objects()  # таблица завершенных объектов автоматически создается
                self.add_supply_materials_func()  # функция триггера на оформление заказа
                self.trigger_add_supply_materials_func()
                self.add_finish_objects_func()  # функция триггера на обновление статус строящихся объектов
                self.trigger_add_finish_object_func()
                self.add_delivered_materials_func()  # функция триггера на доставку заказа
                self.trigger_add_delivered_materials_func()

                print(f"Таблица '{table_name}' создана")
        except Exception as ex:
            print("[ОШИБКА]", ex)
        finally:
            if self.connection:
                print("[INFO] Соединение закрыто")

    def create_table_supply_materials(self):  # вызывается только 1 раз
        """
        Функция создания таблицы supply_materials
        В эту таблицу автоматически будут добавляться все зазказы с таблицы 'materials',
        которые будет оформлять пользователь через меню
        :return:
        """
        with self.connection.cursor() as cursor:
            query = "CREATE TABLE IF NOT EXISTS supply_materials (" \
                    "id serial NOT NULL PRIMARY KEY," \
                    "name text NOT NULL," \
                    "total int NOT NULL," \
                    "orders_qnt int NOT NULL," \
                    "balance int NOT NULL);"
            cursor.execute(query)

    def create_table_delivered_materials(self):  # вызывается только 1 раз
        with self.connection.cursor() as cursor:
            query = "CREATE TABLE IF NOT EXISTS delivered_materials (" \
                    "id serial NOT NULL PRIMARY KEY," \
                    "name text NOT NULL," \
                    "total int NOT NULL," \
                    "orders_qnt int NOT NULL," \
                    "balance int NOT NULL," \
                    "delivery_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            cursor.execute(query)

    def create_table_build_objects(self):  # вызывается только 1 раз
        """
        Функция создания таблицы строящихся объектов с привязкой к id района
        :return:
        """
        with self.connection.cursor() as cursor:
            query = f"CREATE TABLE IF NOT EXISTS build_objects (" \
                    f"id serial NOT NULL PRIMARY KEY," \
                    f"district_id text," \
                    f"object_name text NOT NULL," \
                    f"total_qnt int NOT NULL," \
                    f"status int NOT NULL);"

            cursor.execute(query)
        print(f"Таблица строящихся объектов 'build_objects' создана")

    def create_table_finished_objects(self):  # вызывается только 1 раз
        """
        Функция создания таблицы завершенных объектов.
        В данную таблицу должны автоматически
        добавляться данные из таблицы 'build_objects'
        :return:
        """
        with self.connection.cursor() as cursor:
            query = f"CREATE TABLE IF NOT EXISTS finished_objects (" \
                    f"id serial NOT NULL PRIMARY KEY," \
                    f"district_id text," \
                    f"object_name text NOT NULL," \
                    f"total_qnt int NOT NULL," \
                    f"status int NOT NULL," \
                    f"last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            cursor.execute(query)
            print(f"Таблица завершенных объектов 'finished_objects' создана")

    def create_table_districts(self):  # Вызывается только 1 раз
        """
        Функция создания таблицы районов с кодами
        :return:
        """
        with self.connection.cursor() as cursor:
            query = f"CREATE TABLE IF NOT EXISTS districts (" \
                    f"id serial NOT NULL PRIMARY KEY," \
                    f"name text NOT NULL," \
                    f"district_id text); " \
                    f"INSERT INTO districts (name, district_id) " \
                    f"VALUES" \
                    f"('Октябрьский район', '001')," \
                    f"('Ленинский район', '002')," \
                    f"('Свердловский район', '003')," \
                    f"('Первомайский район', '004');"
            cursor.execute(query)
            print("Таблица районов 'distinct' создана и добавлены значения")

    def view_main_table(self, condition=None):
        """
        Метод отображения списка материалов из базы данных
        :param condition:
        :return:
        """
        try:
            query = "SELECT * FROM %s "
            if condition:
                query += f"WHERE {condition};"

            with self.connection.cursor() as cursor:
                cursor.execute(query % table_name)
                material_list = from_db_cursor(cursor)  # библиотека prettytable - отображение в табличной форме
                cursor.execute(query % table_name)
                records = [row for row in cursor.fetchall()]
                return f'{material_list}\n' \
                       f'В базе данных всего товаров - {len(records)} шт'
        except Exception as ex:
            print("[ОШИБКА]", ex)

    def view_supply_table(self):
        """
        Метод отображения оформленных заказов из таблицы supply_materials
        Данная таблица автоматически обновляется триггером
        :return:
        """
        try:
            query = f"SELECT * FROM supply_materials;"

            with self.connection.cursor() as cursor:
                cursor.execute(query)
                supply_list = from_db_cursor(cursor)  # библиотека prettytable - отображение в табличной форме
                cursor.execute(query)
                records = [row for row in cursor.fetchall()]
                return f"{supply_list}\n" \
                       f"В таблице 'supply_materials' всего заказов - {len(records)}"
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def view_delivered_table(self):
        """
        Метод отображения оформленных заказов из таблицы delivered_materials
        Данная таблица автоматически обновляется триггером
        :return:
        """
        try:
            query = f"SELECT * FROM delivered_materials;"

            with self.connection.cursor() as cursor:
                cursor.execute(query)
                supply_list = from_db_cursor(cursor)
                cursor.execute(query)
                records = [row for row in cursor.fetchall()]
                return f"{supply_list}\n" \
                       f"В таблице 'delivered_materials' всего было доставок - {len(records)}"
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def view_build_objects(self, condition=None):
        """
        Метод отображения строящихся объектов
        :return:
        """
        query = f"SELECT b.*, d.name FROM build_objects b " \
                f"JOIN districts d ON b.district_id = d.district_id "
        if condition:
            query += f"WHERE {condition};"

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            object_list = from_db_cursor(cursor)
            cursor.execute(query)
            records = [row for row in cursor.fetchall()]
            return f'{object_list}\n' \
                   f'Строящихся объектов - {len(records)}'

    def view_finished_objects(self, condition=None):
        """
        Метод отображения завершенных объектов
        :return:
        """
        query = f"SELECT * FROM finished_objects "
        if condition:
            query += f"WHERE {condition};"

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            object_list = from_db_cursor(cursor)
            cursor.execute(query)
            records = [row for row in cursor.fetchall()]
            return f'{object_list}\n' \
                   f'Завершенных объектов - {len(records)}'

    def insert_materials(self):
        """
        Метод добавления материалов в существующую базу данных 'materials'
        Данные считываются с отдельного xlsx-файла
        Названия колонок должно соответствовать коду. ПРОВЕРЯТЬ ПРИ ИСПОЛЬЗОВАНИИ ПРОГРАММЫ!!!
        :return:
        """
        try:
            newData = pd.read_excel(fileName)
            valuesList = newData.values.tolist()
            # print(valuesList)
            listToTuple = [tuple(item) for item in valuesList]
            print(listToTuple)
            new_keys = list(newData)

            with self.connection.cursor() as cursor:
                for key, values in newData.iterrows():
                    insert_new_students = f"INSERT INTO {table_name} ({','.join(new_keys)}) " \
                                          f"VALUES ({','.join(['%s'] * len(values))})"
                    cursor.execute(insert_new_students, tuple(values))
        except Exception as ex:
            print("[ОШИБКА]", ex)
            self.cursor.rollback()
        finally:
            if self.connection:
                print(f'[INFO] добавлены записи {listToTuple}')

    def insert_build_objects(self):
        """
        Функция добавления данных в таблицу строящихся объектов
        :return:
        """
        with self.connection.cursor() as cursor:
            district_id = input('Введите номер района, где строится объект - 001, 002, 003, 004: ')
            object_name = input('Введите название объекта: ')
            total_qnt = int(input('Введите кол-во объектов: '))
            status = int(input('Введите статус объекта 0-в работе, 1-закончен: '))
            query = "INSERT INTO build_objects (district_id, object_name, total_qnt, status) " \
                    "VALUES " \
                    "(%s, %s, %s, %s);"
            cursor.execute(query, (district_id, object_name, total_qnt, status))
            print("Данные в таблицу 'build_objects' занесены")
            self.view_build_objects()

    def update_build_object(self):
        """
        Функция обновления данных в таблице 'build_objects'
        :return:
        """
        try:
            self.view_build_objects()
            object_name = input('Введите название объекта для завершения: ')
            status = int(input('Для завершения объекта введите "1": '))
            with self.connection.cursor() as cursor:
                query = "UPDATE build_objects " \
                        "SET status = %s " \
                        "WHERE object_name = %s"
                cursor.execute(query, (status, object_name))
                self.view_finished_objects()
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def add_finish_objects_func(self):
        """
        Вызывается только 1 раз при создании основной таблицы
        Фукнция для триггера. Данные добавляются автоматически в таблицу finished_objects
        :return:
        """
        with self.connection.cursor() as cursor:
            query = """
                    CREATE OR REPLACE FUNCTION add_finished_object()
                    RETURNS TRIGGER
                    AS $$
                    BEGIN
                        IF TG_OP = 'UPDATE' THEN
							DELETE FROM build_objects
							WHERE status = 1;
                            INSERT INTO finished_objects (district_id, object_name, total_qnt, status, last_updated)
                            VALUES
                            (OLD.district_id, OLD.object_name, OLD.total_qnt, NEW.status, NOW());
                            RETURN NEW;
                        END IF;
                    END;
                    $$ LANGUAGE plpgsql;"""

            cursor.execute(query)

    def trigger_add_finish_object_func(self):
        """
        Вызывается только 1 раз
        Триггер для добавления. Данные добавляются автоматически в таблицу finished_objects
        :return:
        """
        with self.connection.cursor() as cursor:
            query = """
            CREATE OR REPLACE TRIGGER trigger_add_finished_object
            AFTER UPDATE ON build_objects
            FOR EACH ROW
            WHEN (OLD.status IS DISTINCT FROM NEW.status)
            EXECUTE FUNCTION add_finished_object()"""

            cursor.execute(query)

    def add_supply_materials_func(self):
        """
        Вызывается только 1 раз при создании основной таблицы
        Функция для триггера. Данные добавляются автоматически в таблицу supply_materials
        :return:
        """
        with self.connection.cursor() as cursor:
            query = """
        CREATE OR REPLACE FUNCTION add_supply_materials_func()
        RETURNS TRIGGER
        AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN 
                INSERT INTO supply_materials (name, total, orders_qnt, balance)
                VALUES
                (NEW.name, NEW.total, NEW.orders_qnt, NEW.balance);
                RETURN NEW;
            END IF;
        END;
        $$ LANGUAGE plpgsql;"""

            cursor.execute(query)

    def trigger_add_supply_materials_func(self):
        """
        Вызывается только 1 раз
        Триггер для добавления. Lанные добавляются автоматически в таблицу supply_materials

        :return:
        """
        with self.connection.cursor() as cursor:
            query = """
        CREATE OR REPLACE TRIGGER trigger_add_supply_materials_func
        AFTER UPDATE ON materials
        FOR EACH ROW
        WHEN (OLD.balance IS DISTINCT FROM NEW.balance)
        EXECUTE FUNCTION add_supply_materials_func()"""

            cursor.execute(query)

    def add_delivered_materials_func(self):
        """
        Вызывается только 1 раз при создании основной таблицы
        Функция для триггера. Данные добавляются автоматически в таблицу delivered_materials
        :return:
        """
        with self.connection.cursor() as cursor:
            query = """
        CREATE OR REPLACE FUNCTION add_delivered_materials_func()
        RETURNS TRIGGER
        AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN 
                INSERT INTO delivered_materials (name, total, orders_qnt, balance, delivery_time)
                VALUES
                (NEW.name, NEW.total, NEW.orders_qnt, NEW.balance, NOW());
                RETURN NEW;
            END IF;
        END;
        $$ LANGUAGE plpgsql;"""

            cursor.execute(query)

    def trigger_add_delivered_materials_func(self):
        """
        Вызывается только 1 раз
        Триггер для добавления. Lанные добавляются автоматически в таблицу supply_materials

        :return:
        """
        with self.connection.cursor() as cursor:
            query = """
        CREATE OR REPLACE TRIGGER trigger_add_delivered_materials_func
        AFTER UPDATE ON materials
        FOR EACH ROW
        WHEN (OLD.balance IS DISTINCT FROM NEW.balance)
        EXECUTE FUNCTION add_delivered_materials_func()"""

            cursor.execute(query)

    def new_orders(self):
        """Функция оформления заказов.
        Данные обновляются в автоматическом режиме в таблице 'materials'
        Также при обновлении после каждой операции
        данные добавляются в новую таблицу 'supply_materials'
        Для этого есть функции создания ТРИГГЕРА - add_supply_materials_func и trigger_add_supply_materials_func
        """
        try:
            name_value = input('Введите точное название заказа: ')
            set_value = int(input('Введите количество: '))

            with self.connection.cursor() as cursor:
                updateQuery = f"""
                DO $$
                DECLARE 
                    orders int := {set_value};
                BEGIN
                UPDATE {table_name}
                SET orders_qnt = orders_qnt + orders
                WHERE name = '{name_value}';
                
                UPDATE {table_name}
                SET balance = total - orders_qnt
                WHERE name = '{name_value}';
                
                IF (SELECT balance FROM {table_name} WHERE name = '{name_value}') < 0 THEN
                    ROLLBACK;
                    RAISE NOTICE 'Баланс не может быть меньше 0. На складе';
                ELSE
                    RAISE NOTICE 'Успешно';
                    COMMIT;
                END IF;
                END $$;
                """
                cursor.execute(updateQuery)

                print(f'Заказ оформлен:\n'
                      f'Название товара: {name_value}\n'
                      f'Количество: {set_value}')
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def new_deliveries(self):
        """Функция оформления доставок.
        Данные обновляются в автоматическом режиме в таблице 'materials'
        Также при обновлении после каждой операции
        данные добавляются в новую таблицу 'delivered_materials'
        Для этого есть функции создания ТРИГГЕРА - add_delivered_materials_func и trigger_add_delivered_materials_func
        """
        try:
            name_value = input('Введите точное название заказа: ')
            set_value = int(input('Введите количество: '))

            with self.connection.cursor() as cursor:
                updateQuery = f"""
                DO $$
                DECLARE 
                    orders int := {set_value};
                BEGIN
                UPDATE {table_name}
                SET orders_qnt = orders_qnt + orders
                WHERE name = '{name_value}';

                UPDATE {table_name}
                SET balance = total - orders_qnt
                WHERE name = '{name_value}';

                IF (SELECT balance FROM {table_name} WHERE name = '{name_value}') < 0 THEN
                    ROLLBACK;
                    RAISE NOTICE 'Баланс не может быть меньше 0. На складе';
                ELSE
                    RAISE NOTICE 'Успешно';
                    COMMIT;
                END IF;
                END $$;
                """
                cursor.execute(updateQuery)

                time_query = f"SELECT delivery_time FROM delivered_materials WHERE name='{name_value}'"
                cursor.execute(time_query)
                delivered_time = cursor.fetchone()
                fmt = '%Y-%m-%d %H:%M:%S %Z%z'
                for row in delivered_time:
                    date1 = row.strftime(fmt)
                    print(f'Заказ доставлен в {date1}:\n'
                          f'Название товара: {name_value}\n'
                          f'Количество: {set_value} шт')
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def percentage_delivered_func(self):
        with self.connection.cursor() as cursor:
            query = """
            WITH test AS (
            SELECT count(*) AS qnt
            FROM delivered_materials
            UNION
            SELECT count(*)
            FROM materials
            WHERE balance=0
        ) 
        SELECT qnt,
                round(100.0 * qnt / (SELECT sum(qnt) FROM test),2) AS percent,
                SUM(qnt) OVER() as all_delivers
        FROM test;"""
            cursor.execute(query)
            material_list = from_db_cursor(cursor)
            cursor.execute(query)
            records = [row for row in cursor.fetchall()]
            return f'{material_list}\n' \
                   f'Доставленные материалы - {records[0][1]}% или {records[0][0]} шт\n' \
                   f'Нуждающиеся в доставке - {records[1][1]}% или {records[1][0]} шт'

    def min_balance_materials(self):
        """
        Метод отображения материалов с наименьшим количеством
        :return:
        """
        try:
            query = f"SELECT name, balance FROM %s " \
                    f"WHERE balance = (SELECT MIN(balance) " \
                    f"FROM materials WHERE balance != 0)"
            with self.connection.cursor() as cursor:
                cursor.execute(query % table_name)
                records = [row for row in cursor.fetchall()]
                print(f"Всего материалов с наименьшим количеством - "
                      f"{len(records)} позиции\n"
                      f"Список материалов: ")
                return records
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def max_balance_materials(self):
        """
        Метод отображения материалов с наибольшим количеством
        :return:
        """
        try:
            query = f"SELECT name, balance FROM %s " \
                    f"WHERE balance = (SELECT MAX(balance) " \
                    f"FROM materials WHERE balance != 0)"
            with self.connection.cursor() as cursor:
                cursor.execute(query % table_name)
                records = [row for row in cursor.fetchall()]
                print(f"Всего материалов с наибольшим количеством - "
                      f"{len(records)} позиции\n"
                      f"Список материалов: ")
                return records
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def zero_balance_materials(self):
        """
        Метод отображения материалов с количеством 0
        :return:
        """
        try:
            query = f"SELECT id, name, total FROM %s " \
                    f"WHERE balance=0"
            with self.connection.cursor() as cursor:
                cursor.execute(query % table_name)
                records = [row for row in cursor.fetchall()]
                print(f"Всего материалов с нулевым количеством - "
                      f"{len(records)} позиций\n"
                      f"ниже список: ")
                cursor.execute(query % table_name)
                zero_list = from_db_cursor(cursor)
                return zero_list

        except Exception as ex:
            print('[ОШИБКА]', ex)

    def sum_balance_materials(self):
        """
        Метод отображения общего количества материалов
        :return:
        """
        try:
            query = f"SELECT SUM(balance) FROM %s"
            with self.connection.cursor() as cursor:
                cursor.execute(query % table_name)
                records = cursor.fetchone()
                return f"Всего количество материалов: {records[0]} штук"
        except Exception as ex:
            print('[ОШИБКА]', ex)

    def delete_all_tables(self):
        """
        Функция удаления всех таблиц разом.
        :return:
        """
        with self.connection.cursor() as cursor:
            query = f"DROP TABLE materials;" \
                    f"DROP TABLE build_objects;" \
                    f"DROP TABLE finished_objects;" \
                    f"DROP TABLE districts;" \
                    f"DROP TABLE supply_materials;" \
                    f"DROP TABLE delivered_materials;"
                    # f"DROP FUNCTION add_delivered_materials_func;" \
                    # f"DROP FUNCTION add_supply_materials_func;" \
                    # f"DROP FUNCTION add_finished_object;" \
                    # f"DROP TRIGGER trigger_add_supply_materials_func ON materials;" \
                    # f"DROP TRIGGER trigger_add_delivered_materials_func ON materials;" \
                    # f"DROP TRIGGER trigger_add_finished_object ON build_objects;"
            cursor.execute(query)
            print("Удалены следующие таблицы:\n"
                  "- Основная таблица;\n"
                  "-'build_objects';\n"
                  "-'finished_objects';\n"
                  "-'districts';\n"
                  "-'supply_materials';\n"
                  "-'delivered_materials."
                  )


if __name__ == '__main__':
    db = Materials()
    # db.delete_all_tables()
    db.create_main_table()
    # db.insert_materials()
    # print(db.view_finished_objects())
    # print(db.view_build_objects())
