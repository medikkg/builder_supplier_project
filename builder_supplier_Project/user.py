import psycopg2
from config import host, password, port, db_name, user
from prettytable import *
import menu_builder
import menu_supplier

table_name = 'users_PorstgreSQL'
class UserTable:
    def __init__(self):
        self.connection = psycopg2.connect(user=user,
                                           password=password,
                                           host=host,
                                           port=port,
                                           database=db_name)
        self.cursor = self.connection.cursor()
        self.connection.autocommit = True

    def create_table(self):
        with self.connection.cursor() as cursor:
            createTableQuery = f"CREATE TABLE IF NOT EXISTS %s (" \
                               f"id serial NOT NULL PRIMARY KEY, " \
                               f"login text NOT NULL UNIQUE," \
                               f"password text NOT NULL," \
                               f"user_id int" \
                               f"); " \
                               f"CREATE TABLE IF NOT EXISTS users_account (" \
                               f"id serial NOT NULL PRIMARY KEY," \
                               f"user_id int," \
                               f"acc_name text;"
            cursor.execute(createTableQuery % table_name)
            print(f"Таблица '{table_name}' создана\n"
                  f"Также создана доп.таблица 'users_accounts' с названиями аккаунтов")

    def login_and_password(self):
        login = input('Введите логин: ')
        password = input('Введите пароль: ')
        # user_id = int(input('100+: '))
        return login, password

    def insert_user(self):
        with self.connection.cursor() as cursor:
            user_data = self.login_and_password()
            insertQuery = f"INSERT INTO {table_name} " \
                          f"(login, password)" \
                          f"VALUES" \
                          f"(%s, %s);"
            cursor.execute(insertQuery, (user_data))
            print('Данные пользователя успешно занесены!')
            return True

    def selectJoinTable(self):
        with self.connection.cursor() as cursor:
            selectQuery = f"SELECT u.login, u.password, us.acc_name, us.user_id " \
                          f"FROM {table_name} u " \
                          f"JOIN users_account us " \
                          f"ON u.user_id = us.user_id"
            cursor.execute(selectQuery)
            join_table = from_db_cursor(cursor)
            print(join_table)

    def check_users(self, condition=None):
        '''Метод проверяет на соответствие логина, пароля'''
        query = f"SELECT login, password FROM %s "
        if condition:
            query += f"WHERE {condition};"
        with self.connection.cursor() as cursor:
            login, password = self.login_and_password()
            cursor.execute(query % table_name)
            records = [row for row in cursor.fetchall()]
            for record in records:
                if record[0] == login and record[1] == password:
                    return True
            return False


class Login():
    def __init__(self):
        self.check = UserTable()

    def main(self):
        while True:
            print('''Добро пожаловать! Выберите пункт меню:
                        1. Вход
                        2. Регистрация
                        3. Выход 
>>>>> ''')
            user_input = input()
            if user_input == '1':
                choice = input('Введите название аккаунта>>> ')
                if choice == 'builder':
                    result = self.check.check_users(f"user_id=100")
                    if result:
                        print('Вы вошли в систему как Подрядчик')
                        menu_builder.menu_builder()
                    else:
                        print('~~~~~Неверный логин или пароль~~~~~')
                elif choice == 'supplier':
                    result = self.check.check_users(f"user_id=200")
                    if result:
                        print('Вы вошли в систему как Поставщик')
                        menu_supplier.menu_supplier()
                    else:
                        print('~~~~~Неверный логин или пароль~~~~~')
                else:
                    print('~~~~~Нет такого аккаунта~~~~~')
            elif user_input == '2':
                result = self.check.insert_user()
                if not result:
                    print('Пользователь с таким логином уже существует')
                else:
                    print('Регистрация прошла успешно!')

            elif user_input == '3':
                print('Завершение работы')
                break
def create_users_tables():
    db.create_table()


if __name__ == "__main__":
    try:
        fileName = "users.txt"

        db = UserTable()
        manager = Login()
        manager.main()

    except Exception as ex:
        print("[ОШИБКА]", ex)
    finally:
        if db.connection:
            print("[INFO] Соединение закрыто")