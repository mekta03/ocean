# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 09:00:57 2020

@author: vladimir.matveev
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 11:02:33 2020

    !!!!!ВАЖНО!!!!! Работает с подчищенной базой:!!!!!!!

        1. В начале стоят заголовки столбцов
        2. База отсортирована
        3. Дата разбита на столбцы (год, месяц, день)
        4. Удалены левые значения в днях (T16:00 и подобное)



    1.  slice_orig_file(исходна БД, куда будет записан результат)
        cleaning_sliced_file(df)

        Разбивает оригинальный файл БД  Год --> Месяц --> День:
        - в каждом отдельном файле меняет пустые (и неверные) 
           значения глубины места (zz) на глубину последнего горизонта;
        - cобирает все обратно в один файл;
        - удаляет полные дубликаты.    

    2.  outlier_remove(df)

        Удаляет строки с пустыми значениями только кислорода
        Пока НЕ фильтрует "выбросы" в показаниях температуры и солености, кислорода 
        (Можно убрать # и будет работать)

    3.  rounding_levels(df_1)

        Округляет горизонты 

    4.  number_station(df)

        Прописывает номер станции с учетом суточных станций


"""
# TODO: DATETIME
# TODO: УБРАТЬ ВЫБРОСЫ В СОЛЕНОСТИ И ТЕМПЕРАТУРЕ И КИСЛОРОДЕ


import pandas as pd
import collections

user_name = 'egor'
# user_name = 'Vladimir.Matveev'


# path_project = '/mnt/disk_d/УЧЕБА/Самообучение/Програмирование/Python_Projects/Zuenko/Zuenko/py/test/'
path_project = 'D:/УЧЕБА/Самообучение/Програмирование/Python_Projects/Zuenko/Zuenko/py/test/'
path_orig = f'{path_project}new_base.csv'
orig_df = pd.read_csv(path_orig, delimiter=',')

# Пустой Датафрейм с заголовками для присоединения изменённых данных
df_last = pd.DataFrame(data=None, columns=orig_df.columns)


# =============================================================================
# ОКРУГЛЯЮ КООРДИНАТЫ
# =============================================================================
orig_df[['zz', 'level']] = orig_df[['zz', 'level']].round()
orig_df[['long', 'lat']] = orig_df[['long', 'lat']].round(2)
orig_df[['temp', 'sal', 'oxig']] = orig_df[['temp', 'sal', 'oxig']].round(3)
orig_df = orig_df.sort_values(by=['Year', 'Month', 'Day', 'long', 'lat', 'zz', 'level'])


def slice_orig_file(df_1, df_2):
    """
    Выделяет из всей выборки данные по одному дню и дальше обрабатывает их, затем собирает все дни в один файл
    """
    # print(orig_df.describe())

    for year in list(df_1['Year'].unique()):
        for month in list(df_1.query('Year == @year')['Month'].unique()):
            for day in list(df_1.query('Year == @year & Month == @month')['Day'].unique()):
                df_new = df_1.query('Year == @year & Month == @month & Day == @day')  # Выборка по дню

                # Вызов функции замены ZZ (если она равна 0 или меньше глубины посл. горизонта)
                df_new = cleaning_sliced_file(df_new)

                # Записывает обработанный день в пустой датафрейм, в котором только заголовки.
                df_2 = pd.concat([df_2, df_new])

                print(f'{year, month, day}')

    # Удаляет полные дубликаты во всем массиве
    df_2 = df_2.drop_duplicates(['long', 'lat', 'level', 'temp', 'sal', 'oxig'])

    return df_2


def cleaning_sliced_file(df):
    """
    Берет отдельные файлы и меняет в каждом пустой zz на глубину последнего горизонта.
    В случае если ZZ = 0 или меньше глубины посл.горизонта, меняет ZZ на последний горизонт,
    """

    df_clean = df.copy()
    df_clean['zz'] = df_clean['zz'].fillna(0)  # Заменяет пустые значения ZZ на 0

    # Заменяет значения ZZ, которые равны 0 или меньше глубины посл. горизонта, на гулбину последнего горизонта
    grouped_by_coord = df_clean.groupby(by=['long', 'lat'])  # Группирует по координатам
    replaced_null_df = dict(list(grouped_by_coord))  # Записывает результат в словарь

    for k, v in replaced_null_df.items():  # k,v - ключ и значение к словарю, соответственно
        max = v['level'].max()  # беру за максимум глубину последнего горизонта
        # Если глубина места меньше глубины последнего горизонта, соответственно заменяет ее
        for zz in v['zz']:
            if zz < max:
                v['zz'].replace(zz, max, inplace=True)

    # Достаю из словаря новые значения глубины места zz и в итоге записываю их в отдельный список
    lst_zz = []
    for v in replaced_null_df.values():
        for i in v['zz']:
            lst_zz.append(i)

    df_clean['zz'] = lst_zz

    return df_clean


def outlier_remove(df):
    """
    Удаляет выбросы в температуре и солености
    Удаляет строки с пустыми значениями и нулями в кислороде
    """

    # Удаление строк с пустыми значениями и нулями в кислороде
    df_outlier = df.copy()
    df_outlier = df_outlier.dropna(subset=['oxig'])
    df_outlier = df_outlier.query('oxig != 0')

    # Удаление выбросов в температуре и солености
    # df_outlier = df_outlier.query('sal < 40 & sal != 0 & temp < 40')

    return df_outlier


def rounding_levels(df_1):
    """
    Функция замены исходных уровней на округленные

    """

    res = []  # Пустой список для записи округленные уровни
    df = df_1.copy()  # Копия загружаемого файла
    df1 = df['level'].copy()  # Выборка только уровня

    def check_condition(a, b):
        """
        Округляет заданным образом число и добавляет его в созданный ранее список

        """
        boundary = a  # Граница смены дискретности горизонтов [200, 1000, 1500]
        spacing = b  # Интервал горизонтов до 200м - 5м, до 1000м - 25м, до 1500 - 50м, далее 100м

        if spacing == 5:
            if (num % spacing) < 3:
                res.append(num - (num % spacing))
            else:
                res.append(num - (num % spacing) + spacing)

        elif num < boundary + (spacing / 2):
            res.append(boundary)
        elif (boundary + (spacing / 2)) <= num < (boundary + spacing):
            res.append(boundary + spacing)
        else:
            if (num % spacing) == 0:
                res.append(num)
            elif (num % spacing) < spacing / 2:
                res.append(num - (num % spacing))
            else:
                res.append(num - (num % spacing) + spacing)

    # Перебор каждого значения в списке уровней, используя индекс
    for i in range(0, df1.count()):
        # Если это первое значение в списке уровней, то не округляем (НАДО ИЗМЕНИТЬ)
        if i == 0:
            res.append(df1[i])
        # Проверка: является ли данный уровень последним горизонтом, если да, то не округляем, иначе вызываем функцию округления
        elif i > 0 and i != df1.count() - 1:
            if df1[i - 1] < df1[i] > df1[i + 1]:
                res.append(df1[i])
            else:
                num = df1[i]

                if num < 200:
                    check_condition(200, 5)  # Вызов фукнции округления, 200 - граница смены дискретности, 5 - шаг

                elif 200 <= num < 1000:
                    check_condition(200, 25)  # Вызов фукнции округления, 200 - граница смены дискретности, 25 - шаг

                elif 1000 <= num < 1500:
                    check_condition(1000, 50)  # Вызов фукнции округления, 1000 - граница смены дискретности, 50 - шаг

                elif num >= 1500:
                    check_condition(1500, 100)  # Вызов фукнции округления, 1500 - граница смены дискретности, 100 - шаг

        # Если это последнее значение в списке уровней, то не округляем
        elif i == df1.count() - 1:
            res.append(df1[i])

    # print(res)
    serie_level = pd.Series(res)
    df['level'] = serie_level
    df = df.sort_values(by=['Year', 'Month', 'Day', 'long', 'lat', 'zz', 'level'])

    return df


def number_station(df):
    # def number_station():

    """
    Добавляет номер станции, с учетом номера станции в предыдущий день (сквозная нумерация)
    """
    # path = f'C:/Users/{user_name}/Desktop/New_db/Base_of_okhotsk_5.csv'
    # orig_df = pd.read_csv(path, sep =',')	# Делаю копию загружаемого файла

    # # orig_df = orig_df.query('Year == 1991 & ( 152 < long < 157) & (55 < lat < 59) & (Day == 15)')

    orig_df = df.copy()
    global_lst_nst = []  # Пустой список для записи в него номеров станций
    nst = 1  # Начальный номер станции

    for year in list(orig_df['Year'].unique()):
        for month in list(orig_df.query('Year == @year')['Month'].unique()):
            for day in list(orig_df.query('Year == @year & Month == @month')['Day'].unique()):
                # Выборка по дню
                df_new = orig_df.query('Year == @year & Month == @month & Day == @day')

                # Группирую по координатам и уровню
                # grouped_df = df_new.groupby(by=['long', 'lat', 'level'])
                grouped_df = df_new.groupby(by=['long', 'lat', 'zz', 'level'])

                # Преобразую сгруппированный файл в словарь, который сохранет порядок добавления в него элементов
                grouped_df_dict = dict(list(grouped_df))

                # Делаю выборку по координатам и уровню и группирую по координатам
                # grouped_df_2 = df_new[['long','lat','level']].groupby(by=['long', 'lat'])
                grouped_df_2 = df_new[['long', 'lat', 'zz', 'level']].groupby(by=['long', 'lat', 'zz'])

                # Преобразую сгруппированный файл в словарь, который сохранет порядок добавления в него элементов
                grouped_df_dict_2 = dict(list(grouped_df_2))

                # Создаю пустой словарь для записи в него координат - уровня и номеров станций
                dict_1 = collections.OrderedDict()
                # dict_1 = dict() # Обычный тип словаря

                # Создаю вложенный словарь: Коодинаты: {Уровень: [Номера станций]}
                for k, v in grouped_df_dict.items():
                    dict_1[k[:3]] = {}

                # print(dict_1.items())

                """
				Заполняю созданный вложенный словарь:
				Беру значение уровня, делаю проверку: 

				- если при таких координатах такой уровень уже есть,
				тогда вычисляю максимальный номер станции для этих координат и уровня 
				увеличиваю номер станции на 1 
				и записываю новый номер станции в список для этих координат и уровня.

				- если при таких координатах нет такого уровня, 
				добавляю его и записываю для него номер станции,
				установленный по умолчанию в самом начале.

				Затем увеличиваю номер станции на 1.

				"""
                # print(grouped_df_dict_2.items())
                # print()
                # print(dict_1)

                for k, v in grouped_df_dict_2.items():
                    # print(k)
                    # print(v)
                    for level in v['level']:

                        # print(level)
                        # print(dict_1[k].keys())
                        if level in dict_1[k].keys():
                            # print(dict_1[k][level])
                            # print(nst)
                            last_nst = max(dict_1[k][level])
                            dict_1[k][level].append(last_nst + 1)
                            # print(dict_1[k][level])
                            # print(nst)
                        else:
                            dict_1[k][level] = [nst]

                    # print(dict_1.values())
                    num_for_nst = 0
                    for i in dict_1.values():
                        for j in i.values():
                            if max(j) > num_for_nst:
                                num_for_nst = max(j)

                    # print(num_for_nst)
                    nst = num_for_nst + 1

                # Беру номера станций из словаря и добавляю их в созданный ранее список
                for k, v in dict_1.items():
                    for k, v in dict_1[k].items():
                        for num in v:
                            global_lst_nst.append(num)

                """
				Устанавливаю новый номер станции по умолчанию, 
				равный номеру последней станции за этот день плюс 1
				"""
                nst = max(global_lst_nst) + 1
    # Добавляю в таблицу новую колонку с номерами станций
    orig_df['Stations'] = global_lst_nst

    # Записываю полученный результат в отдельный csv файл
    # orig_df.to_csv('C:/Users/Egor/Desktop/oxygen_2.0.2/nst_victory.csv', index=False)

    # Вывожу результат на экран
    return orig_df


# Округлю координаты

name_csv = 'refactoring_base_new.csv'

# Меняет zz и удалет полные дубликаты
new_df_last = slice_orig_file(orig_df, df_last)

# Удаляет пустые значения в t,s,oxig и нулевые значения в Oxig
new_df_last = outlier_remove(new_df_last)

# Вынужденная промежуточна запись в csv, без нее не работает округление горизонтов
new_df_last.to_csv(f'{path_project}/{name_csv}', index=False)

# Считывает промежуточный вариант из csv и записывает его в новую переменную
for_rounded_df = pd.read_csv(f'{path_project}/{name_csv}', sep=',')

# Окргулет горизонты
new_df_last_1 = rounding_levels(for_rounded_df)

# Проставляет номера станций с учетом суточных
new_df_last_1 = number_station(new_df_last_1)

# new_df_last_1 = number_station()


# Записывает результат в Csv
new_df_last_1.to_csv(f'{path_project}/{name_csv}', index=False)

# Говорит, что Вы молодец!
print('\n Mission completed! Good job!\n')
print(new_df_last_1.describe())

"""
if __name__ == '__main__':
    slice_orig_file(orig_df, df_last)
    print(df_last)
    # del_dubl_in_month()
    # new_date()
    # cleaning_sliced_file()
    # number_station()

"""
