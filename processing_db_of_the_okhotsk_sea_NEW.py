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
# path_orig = f'{path_project}new_orig_copy_without_erorr_in_day.csv'
path_orig = f'{path_project}just_for_test.csv'
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


min_year = 1930
max_year = 1931
orig_df = orig_df.query('@min_year <= Year <= @max_year')

# Границы уровней
dct_1 = {i: i + 9 for i in range(0, 31, 10)}
dct_2 = {i: i + 19 for i in range(30, 31)}
dct_3 = {i: i + 49 for i in range(50, 251, 50)}
dct_4 = {i: i + 99 for i in range(300, 1500, 100)}
dct_5 = {i: i + 249 for i in range(1500, 5000, 250)}
dct_std_lvl = {**dct_1, **dct_2, **dct_3, **dct_4, **dct_5}




def replacing_lvl_less_5m_and_more_5m(df):
    """
    Заменяет значение уровня <= 5 м на 0 м,  если нет изначально 0м
    Заменяет значение уровня >  5 м на 10 м, если нет изначально 10м    
    """
    
    new_df = df.copy()

    if 0 <= new_df['Level'].min() < 10:

        if new_df['Level'].min() == 0:
            df_less_5m = new_df.query('Level == 0').copy()

        else:
            # Ближайшее к 0м значение уровня заменяет на 0 
            df_less_5m = new_df.query('Level <= 5').copy()
            df_less_5m['Level'] = 0
            df_less_5m = df_less_5m.head(1)
            

        df_more_5m = new_df.query('Level > 5').copy()

        if 5 < new_df['Level'].max() < 10:
            # Ближайшее к 10м значение уровня заменяет на 10
            new_df.replace(new_df['Level'].max(), 10, inplace=True)
            df_more_5m = new_df.query('Level > 5').copy()
            df_more_5m = df_more_5m.tail(1)

        df_all_lvl = pd.concat([df_more_5m, df_less_5m])
    
    else:
        df_all_lvl = new_df.copy()
    
    cols_for_sort = ['Year', 'Month', 'Day', 'Longitude', 'Latitude', 'Bottom_depth', 'Level']
    df_all_lvl = df_all_lvl.sort_values(by=cols_for_sort)   

    return df_all_lvl


def create_lvl_and_bottom_depth(df):
    """
    Вызывает функцию округления уровней около 0м и около 10м.\n
    Производит линейную интерполяцию.\n
    Производит замену глубины места.\n
    Фильтрует на стд горизонты и посл горизонт.\n

    """
    df = df.copy()
    
    df_all_nst = pd.DataFrame()
    
    counter = 1

    # Перебор каждой станции
    for nst in sorted(df['Station'].unique()):
        print('Осталось проинтерполировать станций ', len(df['Station'].unique())-counter)
        counter += 1

        df_nst = df.query('Station == @nst')

        # ===================================================================
        # Округление уровней около 0 и около 10 м
        # ===================================================================
        df_nst = replacing_lvl_less_5m_and_more_5m(df_nst)


        # ===================================================================
        #! Линейная интерполяция
        # ===================================================================
        df_inter = linear_interpolation.interpolation(df_nst,'Level', [7,8,9])

        # Заполнение непроинтерполированных значений путем их дублирования
        name_cols = ['Longitude','Latitude','Year', 'Month', 'Day','Bottom_depth', 'Station']

        for name in name_cols:
            df_inter[name]= [df_nst[name].iloc[0]]*len(df_inter['Level'])


        # ===================================================================
        #! Замена глубины места
        # ===================================================================
        # Если глубина места=0 или меньше глубины посл.горизонта, меняет ее на глубину посл.горизонта
        if df_inter['Bottom_depth'].max() == 0 or df_inter['Bottom_depth'].max() < df_inter['Level'].max():
            df_inter = df_inter.copy()
            df_inter['Bottom_depth'] = df_inter['Level'].max()


        # ===================================================================
        #! Фильтрация по стандартным горизонтам и последнему горизонту
        # ===================================================================
        std_lvl = dct_std_lvl.keys()

        # Последний горизонт
        max_lvl = df_inter['Level'].max()
        lst_lvl = [*std_lvl, max_lvl]

        df_inter = df_inter.query('Level in @lst_lvl')

        # Объединение всех станций в одну таблицу
        df_all_nst = pd.concat([df_all_nst, df_inter])

    return df_all_nst


def create_number_station(df):
    """
    Создает номера станций:\n
    - группирует по дате, координатам и глубине места,\n
    каждую группу фильтрует по уровням и прописывает номер станции\n
    """
   
    df = df.copy()

    df['Bottom_depth'] = df['Bottom_depth'].fillna(0)

    # Эта группировка только для вывода на печать хода выполнения программы
    df_grouped_1 = df.groupby(by=['Year', 'Month', 'Day','Longitude','Latitude', 'Bottom_depth'])
    counter = 1
    
    df_all_station = pd.DataFrame()

    nst = 1

    for year in sorted(df['Year'].unique()):
        for month in sorted(df.query('Year == @year')['Month'].unique()):
            for day in sorted(df.query('Year == @year and Month == @month')['Day'].unique()):

                df_day = df.query('Year == @year and Month == @month and Day == @day').copy()

                df_grouped = df_day.groupby(by=['Longitude','Latitude', 'Bottom_depth'])
                
                for key in df_grouped.groups.keys():
                    print('Осталось ', len(df_grouped_1.groups.keys()) - counter)
                    counter += 1

                    # Рассматривается поочередо каждая группа
                    df_1_group = df_grouped.get_group(key).copy()

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
                        
                        df_all_station = pd.concat([df_all_station, df_lvl])

                    # Новый номер станции по умолчанию
                    nst = df_all_station['Station'].max() + 1
                    print(nst)
    # df_all_station.to_csv(f'{path_project}just_for_test_numbers_of_nst.csv', index=False)

    return df_all_station


# TODO Создать данную функцию
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


def main():
    # TODO: Проверить получившуюся базу с текущей базой
    # TODO: Удалить выбросы и пропуски
    # TODO: Добавил в удаление дубликатов услови по Bottom_depth надо попробоват поиграть с этим
    # TODO: ЗАмена глубины места из интернета
    new_df = orig_df.drop_duplicates(['Longitude','Latitude', 'Bottom_depth' ,'Level','Temperature', 'Salinity', 'Oxigen'])
    new_df = create_number_station(new_df)
    new_df = create_lvl_and_bottom_depth(new_df)
    print(new_df)
    print(new_df.describe())
    new_df = new_df.copy()
    new_df[['Temperature', 'Salinity', 'Oxigen']] = new_df[['Temperature', 'Salinity', 'Oxigen']].round(3)
    new_df = new_df[['Station',*columns_name,]]
    new_df = new_df.sort_values(by=['Station','Level'])


    new_df.to_csv(f'{path_project}interpolated_{min_year}_{max_year}.csv', index=False)


if __name__ == '__main__':
    main() 
    # pass


