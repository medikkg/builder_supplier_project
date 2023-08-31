import psycopg2
from config import host, password, port, db_name, user

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
                               f");"
            cursor.execute(createTableQuery % table_name)
            print(f"Таблица '{table_name}' создана")

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
            selectQuery = f"SELECT u.login, u.password, us.acc_name " \
                          f"FROM {table_name} u " \
                          f"JOIN users_account us " \
                          f"ON u.user_id = us.user_id"
            cursor.execute(selectQuery)
            records = [row for row in cursor.fetchall()]
            print(records)
    def check_users(self):
        '''Метод проверяет на соответствие логина, пароля'''
        with self.connection.cursor() as cursor:
            login, password = self.login_and_password()
            selectQuery = f"SELECT login, password FROM %s"
            cursor.execute(selectQuery % table_name)
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
                        3. Выход''')
            user_input = input()

            if user_input == '1':
                result = self.check.check_users()
                if result:
                    print('Вы вошли в систему')
                    break
                else:
                    print('~~~~~Неверный логин или пароль~~~~~')

            elif user_input == '2':
                result = self.check.insert_user()
                if not result:
                    print('Пользователь с таким логином уже существует')
                else:
                    print('Регистрация прошла успешно!')

            elif user_input == '3':
                print('Завершение работы')
                break


if __name__ == "__main__":
    try:
        fileName = "users.txt"

        db = UserTable()
        # db.selectJoinTable()
        # db.create_table()
        # db.insert_user()
        # print(db.check_users())
        manager = Login()
        manager.main()

    except Exception as ex:
        print("[ОШИБКА]", ex)
    finally:
        if db.connection:
            print("[INFO] Соединение закрыто")