# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 09:00:57 2020

    !!!!!ВАЖНО!!!!! Работает с подчищенной базой:!!!!!!!

        1. В начале стоят заголовки столбцов
        2. База отсортирована
        3. Дата разбита на столбцы (год, месяц, день)
        4. Удалены левые значения в днях (T16:00 и подобное)


    1.  slice_orig_file(исходна БД, куда будет записан результат)
        making_bottom_depth(df)

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
import linear_interpolation

# Загружает файл БД по Охотскому морю
path_project = '/mnt/disk_d/УЧЕБА/Самообучение/Програмирование/1_Python_Projects/ocean/Profiles_and_means/test/'
path_orig = f'{path_project}new_orig_copy_without_erorr_in_day.csv'
orig_df = pd.read_csv(path_orig, sep=',')

# =============================================================================
# ОКРУГЛЯЮ Значения в таблице
# =============================================================================
orig_df[['zz', 'level']] = orig_df[['zz', 'level']].round()
orig_df[['long', 'lat']] = orig_df[['long', 'lat']].round(2)
orig_df[['temp', 'sal', 'oxig']] = orig_df[['temp', 'sal', 'oxig']].round(3)
# orig_df = orig_df.sort_values(by=['Year', 'Month', 'Day', 'long', 'lat', 'zz', 'level'])

orig_df = orig_df.rename(columns={'long':'Longitude', 'lat':'Latitude', 'zz':'Bottom_depth', 'level':'Level',
                                'temp':'Temperature', 'sal':'Salinity', 'oxig':'Oxigen'})
                                
columns_name = ['Longitude','Latitude','Year', 'Month', 'Day','Bottom_depth', 'Level','Temperature', 'Salinity', 'Oxigen']
orig_df = orig_df[columns_name]
orig_df = orig_df.sort_values(by=['Year', 'Month', 'Day', 'Longitude', 'Latitude', 'Bottom_depth', 'Level'])

# !НАЧАЛО РАСЧЕТА 14:44
min_year = 1930
max_year = 2015
orig_df = orig_df.query('@min_year <= Year <= @max_year')

# Границы уровней
dct_1 = {i: i + 9 for i in range(0, 31, 10)}
dct_2 = {i: i + 19 for i in range(30, 31)}
dct_3 = {i: i + 49 for i in range(50, 251, 50)}
dct_4 = {i: i + 99 for i in range(300, 1500, 100)}
dct_5 = {i: i + 249 for i in range(1500, 5000, 250)}
dct_std_lvl = {**dct_1, **dct_2, **dct_3, **dct_4, **dct_5}


def replacing_lvl_less_5m(df):
    """
    Заменяет значение уровня <= 4 м на 0 м
    """
    
    df_with_lvl_more_5m = df.query('Level >= 5').copy()
    df_with_lvl_less_5m = df.query('Level <= 4').copy()

    df_with_lvl_less_5m = df_with_lvl_less_5m.copy()
    df_with_lvl_less_5m['Level'] = 0

    df_all_lvl_new = pd.concat([df_with_lvl_more_5m, df_with_lvl_less_5m])
    
    cols_for_sort = ['Year', 'Month', 'Day', 'Longitude', 'Latitude', 'Bottom_depth', 'Level']
    df_all_lvl_new = df_all_lvl_new.sort_values(by=cols_for_sort)    
    
    return df_all_lvl_new


# TODO:Поменять имена функций.
def slice_orig_file(df):
    """
    Выделяет из всей выборки данные по одному дню и дальше обрабатывает их, затем собирает все дни в один файл
    """
    df = df.copy()

    # Пустая таблица для записи обработанных данных  
    df_all_time = pd.DataFrame()
    
    for year in sorted(df['Year'].unique()):
        for month in sorted(df.query('Year == @year')['Month'].unique()):
            for day in sorted(df.query('Year == @year & Month == @month')['Day'].unique()):

                # Выборка по дню
                df_day = df.query('Year == @year and Month == @month and Day == @day')  

                # Замена глубины места, если она равна 0 или меньше глубины посл. горизонта
                df_day = making_bottom_depth(df_day)

                # Объединяет обработанные дни в одну таблицу
                df_all_time = pd.concat([df_all_time, df_day])

                print(f'{year, month, day}')

    # Удаляет полные дубликаты во всем массиве
    df_all_time = df_all_time.drop_duplicates(['Longitude','Latitude', 'Level','Temperature', 'Salinity', 'Oxigen'])
    print(df_all_time.query('5 < Level < 10')[['Year', 'Level']])
    print('Stop')
    return df_all_time


def making_bottom_depth(df):
    """
    Меняет глубину места на глубину последнего горизонта, если она = 0 или < глубины посл.горизонта
    """
    print('\n Меняю глубину места и интерполирую\n')
    counter = 1
    df = df.copy()

    # Заменяет отсутствующие значения глубины места на 0
    df['Bottom_depth'] = df['Bottom_depth'].fillna(0)  
    
    df_all_group = pd.DataFrame()

    # Группирует по координатам и в случае необходимости производит замену глубины места для каждой группы.
    grouped_by_coord = df.groupby(by=['Longitude','Latitude']) 
    for key in grouped_by_coord.groups.keys():
        print('осталось ', len(grouped_by_coord.groups.keys()) - counter)
        counter += 1
        df_1_group = grouped_by_coord.get_group(key).copy().reset_index(drop=True)

        # Если глубина места=0 или меньше глубины посл.горизонта, меняет ее на глубину посл.горизонта
        if df_1_group['Bottom_depth'].max() == 0 or df_1_group['Bottom_depth'].max() < df_1_group['Level'].max():
            df_1_group['Bottom_depth'] = df_1_group['Level'].max()
        
        # Производит линейную интерполяцию
        df_inter = linear_interpolation.interpolation(df_1_group,'Level', [7,8,9])
        
        #TODO: Заменить на нормальные стд лвл 
        # std_lvl = [0,10,20,30,50,75,100,125,150,175,200,225,250,275,300]
        std_lvl = dct_std_lvl.keys()

        # Последний горизонт
        max_lvl = df_inter['Level'].max()

        # Оставляет только стд горизонты и последний горизонт
        df_inter = df_inter.query('Level in @std_lvl or Level == @max_lvl')
        
        # Заполнение непроинтерполированных значений путем их дублирования
        name_cols = ['Longitude','Latitude','Year', 'Month', 'Day','Bottom_depth']
        df_inter = df_inter.copy()
        for name in name_cols:
            df_inter[name]= [df_1_group[name].iloc[0]]*len(df_inter['Level'])

        # Объединяет обработанные группы в одну таблицу
        df_all_group = pd.concat([df_all_group, df_inter])
    
    return df_all_group


def making_bottom_depth_2(df):
    """
    Меняет глубину места на глубину последнего горизонта, если она = 0 или < глубины посл.горизонта
    """
    # TODO замена околонулеваого горизонта
    # print('\n Меняю глубину места и интерполирую\n')
    # counter = 1
    df = df.copy()

    # Заменяет отсутствующие значения глубины места на 0
    df['Bottom_depth'] = df['Bottom_depth'].fillna(0)  
    
    df_all_group = pd.DataFrame()

    # Группирует по координатам и в случае необходимости производит замену глубины места для каждой группы.
    grouped_by_coord = df.groupby(by=['Longitude','Latitude']) 
    
    for key in grouped_by_coord.groups.keys():
        # print('осталось ', len(grouped_by_coord.groups.keys()) - counter)
        # counter += 1
        df_1_group = grouped_by_coord.get_group(key).copy()
# =========
        # # Если глубина места=0 или меньше глубины посл.горизонта, меняет ее на глубину посл.горизонта
        # if df_1_group['Bottom_depth'].max() == 0 or df_1_group['Bottom_depth'].max() < df_1_group['Level'].max():
        #     df_1_group['Bottom_depth'] = df_1_group['Level'].max()
            
        # # TODO Если нет 0, но есть около 0 то заменить его на 0
        # # !ПРОБЛЕМА если суточные станции, у одной есть ноль, а другой вместо нуля (например 3), то
        # # ! этот горизонт будет приписан к первой станции, а затем соотвественно удален

        # if 0 not in df_1_group['Level'].unique():
        #    df_1_group = replacing_lvl_less_5m(df_1_group)

        # else:
        #     print('Исключение')
# ============
        # TODO если посл.горизонт меньше 10, но при этом есть ноль - удалить его
        # TODO если нет глубины места, и посл горизонт меньше 10 либо только 0, то и глубина места будет 0

        if df_1_group['Bottom_depth'].empty:
            df_1_group['Bottom_depth'] = df_1_group['Level'].max()



        # Производит линейную интерполяцию
        # df_inter = linear_interpolation.interpolation(df_1_group,'Level', [7,8,9])

        # TODO в суточных станциях не у всех станций сохраняет последний горизонт, 
        # !если у одной из станций последний горизонт был ниже
        # ! последний горизонт может быть удален так как, он может быть приписан не к той станции.
        # ! как решить не знаю


        # TODO Все же добавить обратно округление горизонтов, некоторых 
        # ! Так как есть такие горизонты как 96м, и он является посл. гориз.
        # ? а может быть и нет, т.к. при построении профилей или средних будут использоваться диапазоны значений,
        # ? а не четкие уровни (ВОЗМОЖНО), НО
        # ? будет ли в ODV например при выборке на 100м использоваться 96м или же нет 

        
# ===================
        # # Оставляет только стд горизонты и последний горизонт
        # std_lvl = dct_std_lvl.keys()

        # # Последний горизонт
        # max_lvl = df_inter['Level'].max()
        
        # df_inter = df_inter.query('Level in @std_lvl or Level == @max_lvl')
# ============

        # Заполнение непроинтерполированных значений путем их дублирования
        # name_cols = ['Longitude','Latitude','Year', 'Month', 'Day','Bottom_depth']
        # df_inter = df_inter.copy()
        # for name in name_cols:
        #     df_inter[name]= [df_1_group[name].iloc[0]]*len(df_inter['Level'])
        #     print()
        # # Объединяет обработанные группы в одну таблицу
        # df_all_group = pd.concat([df_all_group, df_inter])

    
    # return df_all_group
    return df_1_group


def bottom_by_nst(df):
    df = df.copy()
    
    df_all_nst = pd.DataFrame()
    counter = 1

    for nst in sorted(df['Station'].unique()):
        print('Осталось проинтерполировать станций ', len(df['Station'].unique())-counter)
        counter += 1
        df_nst = df.query('Station == @nst')

        # Если глубина места=0 или меньше глубины посл.горизонта, меняет ее на глубину посл.горизонта
        if df_nst['Bottom_depth'].max() == 0 or df_nst['Bottom_depth'].max() < df_nst['Level'].max():
            df_nst = df_nst.copy()
            df_nst['Bottom_depth'] = df_nst['Level'].max()

        # # TODO Если нет 0, но есть около 0 то заменить его на 0
        # # !ПРОБЛЕМА если суточные станции, у одной есть ноль, а другой вместо нуля (например 3), то
        # # ! этот горизонт будет приписан к первой станции, а затем соотвественно удален

        if 0 not in df_nst['Level'].unique():
            print('replacing')
            df_nst = replacing_lvl_less_5m(df_nst)
            

        # Производит линейную интерполяцию
        df_inter = linear_interpolation.interpolation(df_nst,'Level', [7,8,9])

        # Заполнение непроинтерполированных значений путем их дублирования
        name_cols = ['Longitude','Latitude','Year', 'Month', 'Day','Bottom_depth', 'Station']
        # df_nst = df_nst.copy()
        for name in name_cols:
            df_inter[name]= [df_nst[name].iloc[0]]*len(df_inter['Level'])
            print()

        
        # Оставляет только стд горизонты и последний горизонт
        std_lvl = dct_std_lvl.keys()

        # Последний горизонт
        max_lvl = df_inter['Level'].max()
        lst_lvl = [*std_lvl, max_lvl]

        df_inter = df_inter.query('Level in @lst_lvl')

        
        df_all_nst = pd.concat([df_all_nst, df_inter])

    return df_all_nst


def create_number_station(df):
    """
    Создает номера станций:\n
    - группирует по дате, координатам и глубине места,\n
    каждую группу фильтрует по уровням и прописывает номер станции\n
    """
    print('\n Создаю номера станций\n')
    
    df = df.copy()

    df['Bottom_depth'] = df['Bottom_depth'].fillna(0)
    df_grouped_1 = df.groupby(by=['Year', 'Month', 'Day','Longitude','Latitude', 'Bottom_depth'])
    
    counter = 1
    
    df_all_station = pd.DataFrame()
    nst = 1
    # TODO Попробовать старую схему
    for year in sorted(df['Year'].unique()):
        for month in sorted(df.query('Year == @year')['Month'].unique()):
            for day in sorted(df.query('Year == @year and Month == @month')['Day'].unique()):
                df_day = df.query('Year == @year and Month == @month and Day == @day').copy()

                # df_day = making_bottom_depth_2(df_day)
                df_day = df_day.copy()
                # df_day['Bottom_depth'] = df_day['Bottom_depth'].fillna(0)

                df_grouped = df_day.groupby(by=['Longitude','Latitude', 'Bottom_depth'])
                
                for key in df_grouped.groups.keys():
                    print('Осталось ', len(df_grouped_1.groups.keys()) - counter)
                    counter += 1
                    # Рассматривается поочередо каждая группа
                    df_1_group = df_grouped.get_group(key).copy()
                    
                    # Оставляет только стд горизонты и последний горизонт
                    std_lvl = dct_std_lvl.keys()

                    # Последний горизонт
                    max_lvl = df_1_group['Level'].max()
                    lst_lvl = [*std_lvl, max_lvl]
        
                    # df_1_group = df_1_group.query('Level in @lst_lvl')
                    # df_1_group = df_1_group.query('Level in @std')
                    
                    df_all_lvl =pd.DataFrame()
                    # Выборка по каждому уровню в текущей группе
                    for lvl in sorted(df_1_group['Level'].unique()):
                        df_lvl = df_1_group.query('Level == @lvl').copy()
                
                        # Расчет номера последней станции для этого уровня 
                        if nst == 1:
                            num_of_station = len(df_lvl['Level']) + 1
                        else:
                            num_of_station = len(df_lvl['Level']) + nst

                        # Запись для этого уровня номеров станций
                        df_lvl['Station'] = range(nst, num_of_station)

                        df_all_lvl = pd.concat([df_all_lvl, df_lvl])

                    # Объединение полученных станции
                    df_all_station = pd.concat([df_all_station, df_all_lvl])
                
                    # Новый номер станции по умолчанию
                    nst = df_all_station['Station'].max() + 1
                    print(nst)
    # df_all_station.to_csv(f'{path_project}numbers_of_nst.csv', index=False)

    return df_all_station


# =============================================================================


    # Номер станции по умолчанию (с которого начинается отсчет)
    nst = 1
    df_grouped = df.groupby(by=['Year', 'Month', 'Day','Longitude','Latitude', 'Bottom_depth'])
    
    for key in df_grouped.groups.keys():
        print('Осталось ', len(df_grouped.groups.keys()) - counter)
        counter += 1

        # Рассматривается поочередо каждая группа
        df_1_group = df_grouped.get_group(key).copy().reset_index(drop=True)

        # Выборка по каждому уровню в текущей группе
        for lvl in sorted(df_1_group['Level'].unique()):
            df_lvl = df_1_group.query('Level == @lvl').copy()
            
            # Расчет номера последней станции для этого уровня 
            if nst == 1:
                num_of_station = len(df_lvl['Level']) + 1
            else:
                num_of_station = len(df_lvl['Level']) + nst

            # Запись для этого уровня номеров станций
            df_lvl['Station'] = range(nst, num_of_station)

            # Объединение полученных станции
            df_all_station = pd.concat([df_all_station, df_lvl])
        
        # Новый номер станции по умолчанию
        nst = df_all_station['Station'].max() + 1
        print(nst)
    # df_all_station.to_csv(f'{path_project}numbers_of_nst.csv', index=False)

    return df_all_station


def outlier_remove(df):
    """
    Удаляет выбросы в температуре и солености
    Удаляет строки с пустыми значениями и нулями в кислороде
    """

    # Удаление строк с пустыми значениями и нулями в кислороде
    df_outlier = df.copy()
    df_outlier = df_outlier.dropna(subset=['Oxigen'])
    df_outlier = df_outlier.query('Oxigen != 0')

    # Удаление выбросов в температуре и солености
    # df_outlier = df_outlier.query('sal < 40 & sal != 0 & temp < 40')

    return df_outlier


def rounding_levels(df_1):
    """
    Функция замены исходных уровней на округленные

    """
    # TODO вместо замены нужно произвести интерполяцию и выборку лишь по стандартным уровням
    res = []  # Пустой список для записи округленные уровни
    df = df_1.copy()  # Копия загружаемого файла
    df1 = df['Level'].copy()  # Выборка только уровня

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

    serie_level = pd.Series(res)
    df['level'] = serie_level
    df = df.sort_values(by=['Year', 'Month', 'Day', 'long', 'lat', 'zz', 'level'])

    return df


def number_station(df):
    """
    Добавляет номер станции, с учетом номера станции в предыдущий день (сквозная нумерация)
    """
    orig_df = df.copy()
    # TODO: Вместо списка с нст создать арифм прогрессию и через next вызвать след номер станции
    global_lst_nst = []  # Пустой список для записи в него номеров станций
    nst = 1  # Начальный номер станции

    for year in sorted(orig_df['Year'].unique()):
        for month in sorted(orig_df.query('Year == @year')['Month'].unique()):
            for day in sorted(orig_df.query('Year == @year and Month == @month')['Day'].unique()):
                # Выборка по дню
                df_new = orig_df.query('Year == @year and Month == @month and Day == @day')
                # TODO: А если сгруппировать сразу по году, месяц, день, координаты
                # Группирую по координатам и уровню
                grouped_df = df_new.groupby(by=['long', 'lat', 'zz', 'level'])

                # Преобразую сгруппированный файл в словарь, который сохранет порядок добавления в него элементов
                grouped_df_dict = dict(list(grouped_df))

                # Делаю выборку по координатам и уровню и группирую по координатам
                grouped_df_2 = df_new[['long', 'lat', 'zz', 'level']].groupby(by=['long', 'lat', 'zz'])

                # Преобразую сгруппированный файл в словарь, который сохранет порядок добавления в него элементов
                grouped_df_dict_2 = dict(list(grouped_df_2))

                # Создаю пустой словарь для записи в него координат - уровня и номеров станций
                dict_1 = collections.OrderedDict()

                # Создаю вложенный словарь: Коодинаты: {Уровень: [Номера станций]}
                for k, v in grouped_df_dict.items():
                    dict_1[k[:3]] = {}

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

                for k, v in grouped_df_dict_2.items():

                    for level in v['level']:
                        if level in dict_1[k].keys():
                            last_nst = max(dict_1[k][level])
                            dict_1[k][level].append(last_nst + 1)
                        else:
                            dict_1[k][level] = [nst]

                    num_for_nst = 0
                    for i in dict_1.values():
                        for j in i.values():
                            if max(j) > num_for_nst:
                                num_for_nst = max(j)

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

    return orig_df


name_csv = 'tested_2.csv'

# Меняет zz и удаляет полные дубликаты
# new_df_last = slice_orig_file(orig_df)

# Удаляет пустые значения в t,s,oxig и нулевые значения в Oxig
# new_df_last = outlier_remove(new_df_last)

# Вынужденная промежуточна запись в csv, без нее не работает округление горизонтов
# new_df_last.to_csv(f'{path_project}/{name_csv}', index=False)

# Считывает промежуточный вариант из csv и записывает его в новую переменную
# for_rounded_df = pd.read_csv(f'{path_project}/{name_csv}', sep=',')

# Окргуляет горизонты
# new_df_last_1 = rounding_levels(for_rounded_df)

# Проставляет номера станций с учетом суточных
# new_df_last_1 = number_station(new_df_last_1)

# Записывает результат в Csv
# new_df_last_1.to_csv(f'{path_project}/{name_csv}', index=False)

# Говорит, что Вы молодец!
# print('\n Mission completed! Good job!\n')
# print(new_df_last_1.describe())


def main():
    # TODO:Проверить получившуюся базу с текущей базой
    # TODO: Удалить выбросы и пропуски

    # new_df = making_bottom_depth_2(orig_df)
    # new_df = making_bottom_depth(orig_df)

    # new_df = new_df.drop_duplicates(['Longitude','Latitude', 'Level','Temperature', 'Salinity', 'Oxigen'])
    new_df = orig_df.drop_duplicates(['Longitude','Latitude', 'Level','Temperature', 'Salinity', 'Oxigen'])
    # new_df = replacing_lvl_less_5m(new_df)
    new_df = create_number_station(new_df)
    new_df = bottom_by_nst(new_df)
    print(new_df)
    print(new_df.describe())
    new_df = new_df.copy()
    new_df[['Temperature', 'Salinity', 'Oxigen']] = new_df[['Temperature', 'Salinity', 'Oxigen']].round(3)
    new_df = new_df[['Station',*columns_name,]]
    new_df = new_df.sort_values(by=['Station','Level'])


    new_df.to_csv(f'{path_project}interpolated_{min_year}_{max_year}.csv', index=False)


if __name__ == '__main__':
    main()
    # slice_orig_file(orig_df)
    
    # del_dubl_in_month()
    # new_date()
    
    # number_station() 
    pass


