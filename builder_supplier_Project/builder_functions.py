from DataBase import *

"""
Группа функций по отображению материалов из базы данных:
"""
db = Materials()


def menu_1():
    """
    Функция отображения Поиск материала:
    1. Найти материал по названию
    2. Найти материал по компании производителя
    3. Найти материал по предназначению
    (Находит материал по определенным критериям из файла
    «materials.txt» где указано название материала, цена, коротко описано
    показания к применению и количество)
    :return:
    """

    subchoice = input("""
    ***Выберите подменю***
    1. Найти материал по названию
    2. Найти материал по компании производителя
    3. Найти материал по предназначению
    >> """)
    match subchoice:
        case '1':
            find_materials_by_name()
        case '2':
            find_material_by_company()
        case '3':
            find_material_by_purpose()
        case _:
            print('Нет такого меню. Попробуйте снова')


def find_materials_by_name():
    choice = input('Введите название продукции: ')
    print(db.view_main_table(f"name='{choice}'"))


def find_material_by_company():
    choice = input('Введите название компании: ')
    print(db.view_main_table(f"company='{choice}'"))


def find_material_by_purpose():
    choice = input('Введите предназначение материала: ')
    print(db.view_main_table(f"purpose='{choice}'"))


def menu_2():
    print(db.view_main_table())


def menu_3():
    """
    Функция отображения запаса материала:
        1. Показать материала с наименьшим количеством
        2. Показать материала с наибольшим количеством
        3. Показать материала, у которого 0 материала
        4. Показать все количество материала
    :return:
    """
    subchoice = input("""
    1. Показать материалы с наименьшим количеством
    2. Показать материалы с наибольшим количеством
    3. Показать материалы, у которого 0 материала
    4. Показать все количество материала
    >> """)
    match subchoice:
        case '1':
            min_balance_func()
        case '2':
            max_balance_func()
        case '3':
            zero_balance_func()
        case '4':
            sum_balance_func()


def menu_4():
    db.new_orders()


def menu_5():
    choice = input("""
    Показать объекты для строительства:
    • Показать все объекты 1
    • Показать по Октябрьскому району 001
    • Показать по Ленинскому району 002
    • Показать по Свердловскому району 003
    • Показать по Первомайскому району 004
    • Добавить новые объекты '+'
    """)
    match choice:
        case '1':
            print(db.view_build_objects())
        case '001':
            print(db.view_build_objects(f"b.district_id='{choice}'"))
        case '002':
            print(db.view_build_objects(f"b.district_id='{choice}'"))
        case '003':
            print(db.view_build_objects(f"b.district_id='{choice}'"))
        case '004':
            print(db.view_build_objects(f"b.district_id='{choice}'"))
        case '+':
            print(db.insert_build_objects())


def menu_6():
    print(db.view_finished_objects())


def menu_7():
    print('Какой объект завершить?>> ')
    menu_5()
    db.update_build_object()
    menu_6()


def min_balance_func():
    print(db.min_balance_materials())


def max_balance_func():
    print(db.max_balance_materials())


def zero_balance_func():
    print(db.zero_balance_materials())


def sum_balance_func():
    print(db.sum_balance_materials())


def menu_0():
    print('ВНИМАНИЕ!!!\n'
          'Эти данные могут быть безвозвратно удалены!!!')
    choice = input('Введите команду для выбора\n'
                   '12345-УДАЛЕНИЕ ВСЕХ ТАБЛИЦ, включая данные\n'
                   '1-Создание базы данных с нуля>> ')
    match choice:
        case '1':
            db.create_main_table()
            db.insert_materials()
        case '12345':
            db.delete_all_tables()


message = """Приветствую дорогой, Подрядчик!
Пожалуйста наберите номер меню для работы с программой, если
закончили, то наберите 9: 
1. Поиск материала
2. Показать полный список материалов
3. Показать запас материала
4. Заказать материал
5. Показать объекты для строительства
6. Показать завершенные объекты
7. Завершить объект
8. Показать информацию о программе (СПРАВКА)
9. Выход
0. Безвозвратное удаление всех данных. ВНИМАНИЕ!
"""

info_message = """
Данная програма предназначен для работы с базой данных строительных материалов
ТЗ имеется в pdf

Основные файлы для работы:
    - DataBase.py - вся работа с базами данных
    - builder_functions.py - меню, подменю, настройка функций ПОДРЯДЧИКА

Название основной таблицы указывается пользователем.
Остальные названия таблиц уже имеются в коде и прописываются автоматически.
1. База данных PostgreSQL и все связующие таблицы создаются автоматически при вызове одной функции
2. Добавление в существующую базу данных - вызов одной функции
3. Удаление полностью всех таблиц и данных - вызов одной функции

Далее после создания таблиц и добавления данных работа ведется только через меню

В программе автоматически создаются триггеры при обновлении данных. Обновленные данные 
автоматически попадают в различные таблицы по соответствующим направлениям
"""

if __name__ == '__main__':
    pass