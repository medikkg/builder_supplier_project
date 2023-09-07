from supplier_functions import *

"""Меню поствщика """
"""Все функции находятся в supplier_functions.py"""


def menu_supplier():
    while True:
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
                pass
                menu_4()
            case '5':
                menu_5()
            case '6':
                print(info_message)
            case '7':
                print('Программа завершена. До новых запросов!')
                exit()
            case 'DROP':
                menu_0()



if __name__ == '__main__':
    menu_supplier()
