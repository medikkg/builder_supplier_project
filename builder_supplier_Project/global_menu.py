from user import *
"""Основное меню"""
"""Выбор меню между Поставщиком и Подрядчиком"""


def main_menu():
    manager = Login()
    manager.main()



if __name__ == '__main__':
    try:

        main_menu()
    except Exception as ex:
        print("[ОШИБКА]", ex)
    finally:
        print("[INFO] Соединение закрыто")