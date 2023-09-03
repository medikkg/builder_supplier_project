from builder_functions import *

"""Меню подрядчика-строителя """
"""Все функции находятся в builder_functions.py"""


def menu_builder():
    print(message)
    choice = input('Введите номер меню от 1 до 9>> ')
    match choice:
        case '1':
            menu_1()
        case '2':
            menu_2()
        case '3':
            menu_3()
        case '4':
            menu_4()
        case '5':
            menu_5()
        case '6':
            menu_6()
        case '7':
            menu_7()
        case '8':
            print(info_message)
        case '9':
            print('Программа завершена. До новых запросов!')
            exit()
        case '0':
            menu_0()


if __name__ == '__main__':
    menu_builder()
