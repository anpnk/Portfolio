  **Выявление профилей потребления для интернет-магазина товаров для дома и быта**  


**Цель:** Необходимо провести исследование, сегментировать покупателей по профилю потребления, по результатам подготовить рекомендации Заказчику для разработки более персонализированных (таргетированных) предложений для покупателей. 



**Основные этапы исследования:**

1. Загрузка и предобработка данных  

    - проверка наличия пропусков;
    - обработка дубликатов;
    - преобразование типов данных;
    - анализ наличия аномалий в данных (проверка на бизнес-правила "1 заказ - 1 клиент" и "1 заказ" - "1 дата").  
    

2. Исследовательский анализ данных

   - Распределение товаров по категориям;
   - Анализ динамики по количеству заказов, по количеству пользователей, расчет среднего чека и т.д.
   
   
3. Сегментация пользователей
      
 - по количеству заказов  
 - по категориям товаров
 - по среднему чеку  
 
 
4. Проверка статистических гипотез  
    
    - Проверка гипотезы об отсутствии отличий между долями клиентов, которые совершают только одну покупку, и более одной покупки.
    - Проверка гипотезы об отсутствии отличий ежемесячной выручки в декабре и ноябре.
    

5. Презентация

     
6. Выводы
   



### Загрузка и предобработка данных

#### Импорт библиотек и настройка опций


```python

#---------------------------
# загрузка библиотек

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from matplotlib import pyplot as plt


import os
#import pandas as pd
#import matplotlib.pyplot as plt
import scipy.stats as st
#import numpy as np
import seaborn as sns

pd.set_option('display.max_columns', 50)
sns.color_palette("pastel")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 150)
pd.set_option('display.max_rows', None)

import warnings
warnings.filterwarnings("ignore")   

import plotly.express as px

None

```

#### Вспомогательные функции


```python

#--------- функция загрузки датафрейма

def load_df(pth1, pth2):
    global df
    if os.path.exists(pth1):
        df = pd.read_csv(pth1, sep=",")
        print()
        print('Файл загружен: ', pth1)  
        print()
    elif os.path.exists(pth2):
        df = pd.read_csv(pth2, sep=",")
        print()
        print('Файл загружен: ', pth2)        
        print()
    else:
        print('Ошибка при загрузке датафрейма, проверьте путь расположения файла.')
    return df

```


```python

#--------- функция проверки пропусков в датафрейме

def check_df(df, df_name, percent_info=False):
    
    df.attrs['name'] = df_name 
    
    
    #---------------------------
    # Проверяем наличие пропусков в датафрейме
    
    print('----------------------')
    print('Проверка пропусков в датафрейме <', df.attrs['name'], '>: ')
    print(df.isna().sum())
    print()
    
    #---------------------------
    # Подсчитываем процент пропусков в каждом столбце
    
    if percent_info==True:
        print('Процент пропущенных данных по каждому столбцу:')
        print()
        print(df.isna().sum() / df.isna().count()*100) 
        print()
    print('----------------------')
    
    return 

```

#### Загрузка файла с данными


```python

#---------------------------
# определяем основной и альтернативный пути к расположению датасета

path_1 = 'C:/Users/user/Downloads/ecom_dataset_upd.csv'
path_1_alter = '/datasets/ecom_dataset_upd.csv'


#---------------------------
# загрузка файла c данными 

df = load_df(path_1, path_1_alter)

```

    
    Файл загружен:  C:/Users/user/Downloads/ecom_dataset_upd.csv
    
    


#### Анализ датасета: загрузка общей информации, предварительный просмотр данных
    


```python

df.info()

```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 7474 entries, 0 to 7473
    Data columns (total 6 columns):
     #   Column       Non-Null Count  Dtype  
    ---  ------       --------------  -----  
     0   date         7474 non-null   int64  
     1   customer_id  7474 non-null   object 
     2   order_id     7474 non-null   int64  
     3   product      7474 non-null   object 
     4   quantity     7474 non-null   int64  
     5   price        7474 non-null   float64
    dtypes: float64(1), int64(3), object(2)
    memory usage: 350.5+ KB
    


```python

df.head(25)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Комнатное растение в горшке Алое Вера, d12, h30</td>
      <td>1</td>
      <td>142.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Комнатное растение в горшке Кофе Арабика, d12, h25</td>
      <td>1</td>
      <td>194.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Радермахера d-12 см h-20 см</td>
      <td>1</td>
      <td>112.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Хризолидокарпус Лутесценс d-9 см</td>
      <td>1</td>
      <td>179.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Циперус Зумула d-12 см h-25 см</td>
      <td>1</td>
      <td>112.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Шеффлера Лузеана d-9 см</td>
      <td>1</td>
      <td>164.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2018100100</td>
      <td>ee47d746-6d2f-4d3c-9622-c31412542920</td>
      <td>68477</td>
      <td>Юкка нитчатая d-12 см h-25-35 см</td>
      <td>1</td>
      <td>134.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2018100108</td>
      <td>375e0724-f033-4c76-b579-84969cf38ee2</td>
      <td>68479</td>
      <td>Настенная сушилка для белья Gimi Brio Super 100</td>
      <td>1</td>
      <td>824.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2018100108</td>
      <td>6644e5b4-9934-4863-9778-aaa125207701</td>
      <td>68478</td>
      <td>Таз пластмассовый 21,0 л круглый "Водолей" С614, 1404056</td>
      <td>1</td>
      <td>269.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2018100109</td>
      <td>c971fb21-d54c-4134-938f-16b62ee86d3b</td>
      <td>68480</td>
      <td>Чехол для гладильной доски Colombo Persia Beige 130х50 см из хлопка 5379</td>
      <td>1</td>
      <td>674.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2018100111</td>
      <td>161e1b98-45ba-4b4e-8236-e6e3e70f6f7c</td>
      <td>68483</td>
      <td>Вешалка для брюк металлическая с резиновым покрытием 26 см цвет: синяя, Attribute, AHS331</td>
      <td>10</td>
      <td>82.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2018100112</td>
      <td>86432d8d-b706-463b-bd5d-6a9e170daee3</td>
      <td>68484</td>
      <td>Сушилка для белья потолочная Zalger Lift Basic 1520 200 см, 10 м</td>
      <td>1</td>
      <td>614.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2018100113</td>
      <td>4d93d3f6-8b24-403b-a74b-f5173e40d7db</td>
      <td>68485</td>
      <td>Чехол Eurogold Clean Basic хлопок для досок 120х38-120х42 см C42</td>
      <td>1</td>
      <td>187.0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2018100115</td>
      <td>0948b0c2-990b-4a11-b835-69ac4714b21d</td>
      <td>68486</td>
      <td>Крючок одежный 2-х рожковый серый металлик с полимерным покрытием *Тонар*, 1110027</td>
      <td>96</td>
      <td>38.0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2018100116</td>
      <td>a576fa59-7b28-4a4c-a496-92f128754a94</td>
      <td>68487</td>
      <td>Корзина мягкая пластиковая 17 л, М-пластика, M2880</td>
      <td>1</td>
      <td>188.0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2018100118</td>
      <td>17213b88-1514-47a4-b8aa-ce51378ab34e</td>
      <td>68476</td>
      <td>Мини-сковорода Marmiton "Сердце" с антипригарным покрытием 12 см, LG17085</td>
      <td>1</td>
      <td>239.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2018100118</td>
      <td>17213b88-1514-47a4-b8aa-ce51378ab34e</td>
      <td>68476</td>
      <td>Сковорода алюминиевая с антипригарным покрытием MARBLE ALPENKOK d = 26 см AK-0039A/26N</td>
      <td>1</td>
      <td>824.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2018100118</td>
      <td>17213b88-1514-47a4-b8aa-ce51378ab34e</td>
      <td>68476</td>
      <td>Стеклянная крышка для сковороды ALPENKOK 26 см AK-26GL</td>
      <td>1</td>
      <td>262.0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2018100118</td>
      <td>17213b88-1514-47a4-b8aa-ce51378ab34e</td>
      <td>68476</td>
      <td>Сушилка для белья напольная Colombo Star 18, 3679</td>
      <td>1</td>
      <td>1049.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2018100121</td>
      <td>b731df05-98fa-4610-8496-716ec530a02c</td>
      <td>68474</td>
      <td>Доска гладильная Eurogold Professional 130х48 см металлическая сетка 35748W</td>
      <td>1</td>
      <td>3299.0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2018100208</td>
      <td>c971fb21-d54c-4134-938f-16b62ee86d3b</td>
      <td>68490</td>
      <td>Чехол для гладильной доски Festival 137x60 см из хлопка 4738</td>
      <td>1</td>
      <td>1162.0</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2018100210</td>
      <td>4d93d3f6-8b24-403b-a74b-f5173e40d7db</td>
      <td>68491</td>
      <td>Сумка-тележка 2-х колесная Gimi Argo синяя</td>
      <td>1</td>
      <td>1049.0</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2018100211</td>
      <td>f08d9018-438e-4e96-b519-f74c0302a433</td>
      <td>14480</td>
      <td>Многолетнее растение Тимьян-чабрец розовый объем 0,5 л</td>
      <td>1</td>
      <td>89.0</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2018100211</td>
      <td>f08d9018-438e-4e96-b519-f74c0302a433</td>
      <td>14480</td>
      <td>Рассада зелени для кухни Базилик Тонус, кассета по 6шт</td>
      <td>1</td>
      <td>169.0</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2018100211</td>
      <td>f08d9018-438e-4e96-b519-f74c0302a433</td>
      <td>14480</td>
      <td>Рассада зелени для кухни Мелиссы в горшке диам. 9 см</td>
      <td>1</td>
      <td>101.0</td>
    </tr>
  </tbody>
</table>
</div>




```python

df.tail(25)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>7449</th>
      <td>2020012713</td>
      <td>d3e39f6c-9d17-49ee-aa32-3e78cbeb6404</td>
      <td>106214</td>
      <td>Орехоколка VIVA, Attribute, ATV600</td>
      <td>1</td>
      <td>239.0</td>
    </tr>
    <tr>
      <th>7450</th>
      <td>2020012714</td>
      <td>ae036d1b-b1d5-41e4-8c85-d2bd1b0b4e18</td>
      <td>101880</td>
      <td>Настурция Орхидное пламя 5 шт 4650091480692</td>
      <td>1</td>
      <td>14.0</td>
    </tr>
    <tr>
      <th>7451</th>
      <td>2020012714</td>
      <td>c23c92a7-c8d8-46b4-8511-8efdf90d6a55</td>
      <td>105214</td>
      <td>Муляж Вишня 3 см 10 шт полиуретан</td>
      <td>2</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>7452</th>
      <td>2020012722</td>
      <td>aae3b3d2-8ab8-4558-a141-e23f0b77e63d</td>
      <td>106325</td>
      <td>Сушилка для белья напольная COLOMBO ALPINA 30MT ST194/3CF</td>
      <td>1</td>
      <td>1574.0</td>
    </tr>
    <tr>
      <th>7453</th>
      <td>2020012722</td>
      <td>43ff529f-8ff0-473d-8168-4b730137a2e1</td>
      <td>111991</td>
      <td>Цветок искусственный Василек 3 цветка 55 см пластик</td>
      <td>1</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>7454</th>
      <td>2020012811</td>
      <td>ea0d1348-249e-4aff-9149-599eb6d98dec</td>
      <td>106547</td>
      <td>Настурция Орхидное пламя 5 шт 4650091480692</td>
      <td>1</td>
      <td>14.0</td>
    </tr>
    <tr>
      <th>7455</th>
      <td>2020012811</td>
      <td>eeba9850-8d4b-40a7-a110-6d11a9196ddc</td>
      <td>103325</td>
      <td>Кружка для чая Сердечки 250 мл ДСГ55029146</td>
      <td>1</td>
      <td>45.0</td>
    </tr>
    <tr>
      <th>7456</th>
      <td>2020012814</td>
      <td>57c8999f-ebad-485c-985e-9dc99837775d</td>
      <td>112325</td>
      <td>Рассада Капусты белокачанная сорт Сибирячка кассета 6 шт E6</td>
      <td>2</td>
      <td>120.0</td>
    </tr>
    <tr>
      <th>7457</th>
      <td>2020012815</td>
      <td>92b18c53-a3d3-44cf-9492-a3566017d611</td>
      <td>106880</td>
      <td>Форма для выпечки ALPENKOK AK-6038S для кекса Спираль розовый d=25 см; h=9 см</td>
      <td>1</td>
      <td>254.0</td>
    </tr>
    <tr>
      <th>7458</th>
      <td>2020012815</td>
      <td>04d4b824-7b31-4262-8aa0-e43772602521</td>
      <td>110214</td>
      <td>Тарелка обеденная КОТОН ФЛАУЭР 25 см H2776 Luminarc</td>
      <td>1</td>
      <td>90.0</td>
    </tr>
    <tr>
      <th>7459</th>
      <td>2020012818</td>
      <td>75c1d45a-be89-4f94-8c38-0d386f718943</td>
      <td>102547</td>
      <td>Вешалка-сушилка Gimi Paco</td>
      <td>1</td>
      <td>1087.0</td>
    </tr>
    <tr>
      <th>7460</th>
      <td>2020012821</td>
      <td>709dabff-4d9a-4ab0-9e1c-39c253bf90ad</td>
      <td>105880</td>
      <td>Сумка-тележка хозяйственная Rolser Paris, бордовая, PEP001 bassi JOY</td>
      <td>1</td>
      <td>4117.0</td>
    </tr>
    <tr>
      <th>7461</th>
      <td>2020012823</td>
      <td>c420236c-ebad-4027-a27d-a3e92fbfe11d</td>
      <td>109214</td>
      <td>Цинерания рассада однолетних цветов в кассете по 10 шт</td>
      <td>1</td>
      <td>210.0</td>
    </tr>
    <tr>
      <th>7462</th>
      <td>2020012911</td>
      <td>9777b839-4212-41bb-94c2-87de3658248a</td>
      <td>111436</td>
      <td>Крючок проволочный 120 мм оцинкованный, 1110212</td>
      <td>1</td>
      <td>15.0</td>
    </tr>
    <tr>
      <th>7463</th>
      <td>2020012912</td>
      <td>331e6879-1974-48fb-b8c4-65f03edbb64d</td>
      <td>112547</td>
      <td>Коврик интерьерный для кухни Tuscan Wine из ПВХ прямоугольный 45х75 см, Apache, 4660</td>
      <td>1</td>
      <td>1012.0</td>
    </tr>
    <tr>
      <th>7464</th>
      <td>2020012913</td>
      <td>28437f82-c2a8-41ea-a7c1-bcedece59d8b</td>
      <td>102658</td>
      <td>Гладильная доска НИКА 3+ 122х34,5 см Н3+</td>
      <td>1</td>
      <td>1124.0</td>
    </tr>
    <tr>
      <th>7465</th>
      <td>2020012913</td>
      <td>0b2157e5-101e-4e0e-bfaf-7340ed23e574</td>
      <td>111547</td>
      <td>Коврик придверный, полипропилен, 40х70 см, Kokarda, Пес и кот, бежевый, PHP SRL, 21417</td>
      <td>1</td>
      <td>749.0</td>
    </tr>
    <tr>
      <th>7466</th>
      <td>2020012914</td>
      <td>904015ba-31f2-4ce4-b68e-02362280a43d</td>
      <td>107214</td>
      <td>Ящик почтовый металлический с ушками для навесного замка Домик 1205251</td>
      <td>1</td>
      <td>172.0</td>
    </tr>
    <tr>
      <th>7467</th>
      <td>2020012917</td>
      <td>4228e34b-dcba-4df8-ae70-b282e84a1edb</td>
      <td>110547</td>
      <td>Tepмокружка AVEX Freeflow 700 мл зеленый AVEX0759</td>
      <td>1</td>
      <td>2399.0</td>
    </tr>
    <tr>
      <th>7468</th>
      <td>2020013008</td>
      <td>370ed405-57f6-4eff-ab2e-a0bacab6e982</td>
      <td>102891</td>
      <td>Пеларгония зональная Ринго Вайт d-7 см h-10 см укорененный черенок</td>
      <td>1</td>
      <td>74.0</td>
    </tr>
    <tr>
      <th>7469</th>
      <td>2020013021</td>
      <td>63208953-a8e4-4f77-9b47-3a46e7b72eee</td>
      <td>104002</td>
      <td>томата (помидор) Черниченский черри № 116 сорт индетерминантный позднеспелый черный</td>
      <td>2</td>
      <td>38.0</td>
    </tr>
    <tr>
      <th>7470</th>
      <td>2020013022</td>
      <td>d99d25f1-4017-4fcd-8d29-c580cc695a1a</td>
      <td>107336</td>
      <td>Дендробиум Санок Анна Грин 1 ствол d-12 см</td>
      <td>1</td>
      <td>869.0</td>
    </tr>
    <tr>
      <th>7471</th>
      <td>2020013102</td>
      <td>2c9bd08d-8c55-4e7a-9bfb-8c56ba42c6d6</td>
      <td>106336</td>
      <td>Подставка для обуви резиновая Attribute 80x40 см AMC080</td>
      <td>1</td>
      <td>354.0</td>
    </tr>
    <tr>
      <th>7472</th>
      <td>2020013112</td>
      <td>cdd17932-623e-415f-a577-3b31312fd0e2</td>
      <td>102002</td>
      <td>Тагетис крупноцветковый рассада однолетних цветов в кассете по 6 шт</td>
      <td>1</td>
      <td>128.0</td>
    </tr>
    <tr>
      <th>7473</th>
      <td>2020013115</td>
      <td>2e460a26-35af-453d-a369-a036e95a40e0</td>
      <td>103225</td>
      <td>Вешалка для блузок 41 см красный Attribute AHM781</td>
      <td>1</td>
      <td>104.0</td>
    </tr>
  </tbody>
</table>
</div>




```python

check_df(df,'ecom_dataset_upd')
    
```

    ----------------------
    Проверка пропусков в датафрейме < ecom_dataset_upd >: 
    date           0
    customer_id    0
    order_id       0
    product        0
    quantity       0
    price          0
    dtype: int64
    
    ----------------------
    

**Выводы**  

**Предварительный анализ датафрейма показывает:**  


- всего строк: 7474;  


- пропуски в столбцах отсутствуют;


- преобразование названий столбцов к нижнему регистру и «snake_case» не требуется.  


- необходимо скорректировать тип столбца `date` на *datetime*.



#### Проверка явных дубликатов в датафрейме  


```python

df.duplicated().sum()

```




    0



Явные дубликаты отсутствуют.

#### Проверка наличия в датафрейме неявных дубликатов

Проанализируем столбец **`customer_id`**.


```python

print(len(df['customer_id'].unique()))
df['customer_id'].unique()

```

    2451
    




    array(['ee47d746-6d2f-4d3c-9622-c31412542920',
           '375e0724-f033-4c76-b579-84969cf38ee2',
           '6644e5b4-9934-4863-9778-aaa125207701', ...,
           'f17ed857-178e-45e1-a662-0a9dd3b58c5f',
           '1f0a7f35-7459-4f23-b468-5e45bf481dd1',
           '25df96a7-c453-4708-9cea-a3dfc7c342ea'], dtype=object)




```python

#-----преобразуем к верхнему регистру

df['customer_id'] = df['customer_id'].str.upper()
print(len(df['customer_id'].unique()))
df['customer_id'].unique()

```

    2451
    




    array(['EE47D746-6D2F-4D3C-9622-C31412542920',
           '375E0724-F033-4C76-B579-84969CF38EE2',
           '6644E5B4-9934-4863-9778-AAA125207701', ...,
           'F17ED857-178E-45E1-A662-0A9DD3B58C5F',
           '1F0A7F35-7459-4F23-B468-5E45BF481DD1',
           '25DF96A7-C453-4708-9CEA-A3DFC7C342EA'], dtype=object)



После преобразования к верхнему регистру количество уникальных значений `customer_id` не изменилось - 2451. Неявные дубликаты по полю `customer_id` отсутствуют.

Проанализируем столбец **`product`**.


```python

df_src = pd.Series(df['product'])
print(len(df['product'].unique()))
df['product'].unique()

```

    2343
    




    array(['Комнатное растение в горшке Алое Вера, d12, h30',
           'Комнатное растение в горшке Кофе Арабика, d12, h25',
           'Радермахера d-12 см h-20 см', ...,
           'Сушилка для белья на ванну FREUDENBERG (GIMI) Alablock Varadero silver A4P',
           'Каланхое каландива малиновое d-7 см', 'Литопс Микс d-5 см'],
          dtype=object)



На начальном этапе количество уникальных значений  - 2343.   
Сформируем сводную таблицу для оценки количества уникальных наименований товаров: 


```python

df_src_product = df.pivot_table(index='product', values='quantity', \
                                aggfunc='sum').sort_values(by='quantity', ascending=False).reset_index()

print(len(df_src_product))
df_src_product.head(5)

```

    2343
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Вантуз с деревянной ручкой d14 см красный, Burstenmann, 0522/0000</td>
      <td>1000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Муляж ЯБЛОКО 9 см красное</td>
      <td>618</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Вешалки мягкие для деликатных вещей 3 шт шоколад</td>
      <td>335</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Крепеж для пружины дверной, 1107055</td>
      <td>320</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Муляж Яблоко зеленый 9 см полиуретан</td>
      <td>308</td>
    </tr>
  </tbody>
</table>
</div>



Преобразуем наименования к верхнему регистру:



```python

df_src_product['product'] = df_src_product['product'].str.upper()

print(len(df_src_product))
df_src_product.head(5)

```

    2343
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ВАНТУЗ С ДЕРЕВЯННОЙ РУЧКОЙ D14 СМ КРАСНЫЙ, BURSTENMANN, 0522/0000</td>
      <td>1000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>МУЛЯЖ ЯБЛОКО 9 СМ КРАСНОЕ</td>
      <td>618</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ВЕШАЛКИ МЯГКИЕ ДЛЯ ДЕЛИКАТНЫХ ВЕЩЕЙ 3 ШТ ШОКОЛАД</td>
      <td>335</td>
    </tr>
    <tr>
      <th>3</th>
      <td>КРЕПЕЖ ДЛЯ ПРУЖИНЫ ДВЕРНОЙ, 1107055</td>
      <td>320</td>
    </tr>
    <tr>
      <th>4</th>
      <td>МУЛЯЖ ЯБЛОКО ЗЕЛЕНЫЙ 9 СМ ПОЛИУРЕТАН</td>
      <td>308</td>
    </tr>
  </tbody>
</table>
</div>



Проверим наличие дубликатов в этом поле после преобразования:


```python

df_src_product['product'].duplicated().sum()

```




    2




```python

duplicateRows = df_src_product[df_src_product.duplicated(['product'], keep=False)]
duplicateRows

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>937</th>
      <td>РОЗА КОРДАНА БЕЛАЯ D-10 СМ H-20</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1126</th>
      <td>РОЗА ПАТИО ОРАНЖЕВАЯ D-12 СМ H-30</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2107</th>
      <td>РОЗА КОРДАНА БЕЛАЯ D-10 СМ H-20</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2117</th>
      <td>РОЗА ПАТИО ОРАНЖЕВАЯ D-12 СМ H-30</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



По 2-м позициям товаров фиксируем дублирование идентичных наименований. Для корректного учета наименований при дальнейшем анализе данных преобразуем к верхнему регистру наименования в этом поле в исходном датафрейме:


```python

#-----преобразуем к верхнему регистру

df['product'] = df['product'].str.upper()
print(len(df['product'].unique()))
df['product'].unique()

df_Upper_product = pd.Series(df['product'])

```

    2341
    

После преобразования выполним повторную проверку наличия явных дубликатов:


```python

df.duplicated().sum()

```




    0




**Дубликаты отсутствуют.**



```python

df.head(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018100100</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ АЛОЕ ВЕРА, D12, H30</td>
      <td>1</td>
      <td>142.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018100100</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ КОФЕ АРАБИКА, D12, H25</td>
      <td>1</td>
      <td>194.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018100100</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>РАДЕРМАХЕРА D-12 СМ H-20 СМ</td>
      <td>1</td>
      <td>112.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018100100</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ХРИЗОЛИДОКАРПУС ЛУТЕСЦЕНС D-9 СМ</td>
      <td>1</td>
      <td>179.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018100100</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ЦИПЕРУС ЗУМУЛА D-12 СМ H-25 СМ</td>
      <td>1</td>
      <td>112.0</td>
    </tr>
  </tbody>
</table>
</div>



Оценим количество уникальных заказов в исходном датасете:


```python

print(len(df['order_id'].unique()))

```

    3521
    


#### Проверка бизнес-правила "1 заказ - 1 клиент"


Проверим датасет на наличие строк, когда одному номеру заказа соответствует несколько идентификаторов клиентов:


```python

df_order_customer_check = df.groupby(['order_id']).agg({'customer_id' : 'nunique'}).reset_index()
df_order_customer_check.columns = ['order_id', 'customer_id_counter']

df_order_customer_check = \
        df_order_customer_check.query('customer_id_counter > 1').sort_values(by='customer_id_counter', ascending=False)

orders_delete_list = df_order_customer_check['order_id']

df_order_customer_check.info()
df_order_customer_check

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 29 entries, 2610 to 2677
    Data columns (total 2 columns):
     #   Column               Non-Null Count  Dtype
    ---  ------               --------------  -----
     0   order_id             29 non-null     int64
     1   customer_id_counter  29 non-null     int64
    dtypes: int64(2)
    memory usage: 696.0 bytes
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2610</th>
      <td>72845</td>
      <td>4</td>
    </tr>
    <tr>
      <th>902</th>
      <td>69485</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1914</th>
      <td>71480</td>
      <td>3</td>
    </tr>
    <tr>
      <th>248</th>
      <td>14872</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1651</th>
      <td>70946</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2576</th>
      <td>72790</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2569</th>
      <td>72778</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2269</th>
      <td>72188</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2004</th>
      <td>71663</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2000</th>
      <td>71648</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1956</th>
      <td>71571</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1939</th>
      <td>71542</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1905</th>
      <td>71461</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1799</th>
      <td>71226</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1712</th>
      <td>71054</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1631</th>
      <td>70903</td>
      <td>2</td>
    </tr>
    <tr>
      <th>516</th>
      <td>68785</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1581</th>
      <td>70808</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1542</th>
      <td>70726</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1495</th>
      <td>70631</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1443</th>
      <td>70542</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1200</th>
      <td>70114</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1066</th>
      <td>69833</td>
      <td>2</td>
    </tr>
    <tr>
      <th>933</th>
      <td>69531</td>
      <td>2</td>
    </tr>
    <tr>
      <th>862</th>
      <td>69410</td>
      <td>2</td>
    </tr>
    <tr>
      <th>832</th>
      <td>69345</td>
      <td>2</td>
    </tr>
    <tr>
      <th>817</th>
      <td>69310</td>
      <td>2</td>
    </tr>
    <tr>
      <th>797</th>
      <td>69283</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2677</th>
      <td>72950</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python

print('')
print('Всего строк в исходном датасете:', len(df))
print('Строк к удалению:', len(df) - len(df.query('order_id not in @orders_delete_list')))
print('% строк в удалению:', (len(df) - len(df.query('order_id not in @orders_delete_list')))/len(df)*100)
print('')

```

    
    Всего строк в исходном датасете: 7474
    Строк к удалению: 89
    % строк в удалению: 1.1907947551511908
    
    

Присутствует 29 "проблемных" заказов, что соответствует 89 транзакциям в датасете и составляет порядка 1.2%.  
Чтобы исключить искажение результатов (смещение статистик) удалим такие заказы (все транзакции с этими заказами) из датасета.


```python

df.info()

```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 7474 entries, 0 to 7473
    Data columns (total 6 columns):
     #   Column       Non-Null Count  Dtype  
    ---  ------       --------------  -----  
     0   date         7474 non-null   int64  
     1   customer_id  7474 non-null   object 
     2   order_id     7474 non-null   int64  
     3   product      7474 non-null   object 
     4   quantity     7474 non-null   int64  
     5   price        7474 non-null   float64
    dtypes: float64(1), int64(3), object(2)
    memory usage: 350.5+ KB
    


```python

df = df.query('order_id not in @orders_delete_list')
df.info()

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 7385 entries, 0 to 7473
    Data columns (total 6 columns):
     #   Column       Non-Null Count  Dtype  
    ---  ------       --------------  -----  
     0   date         7385 non-null   int64  
     1   customer_id  7385 non-null   object 
     2   order_id     7385 non-null   int64  
     3   product      7385 non-null   object 
     4   quantity     7385 non-null   int64  
     5   price        7385 non-null   float64
    dtypes: float64(1), int64(3), object(2)
    memory usage: 403.9+ KB
    


#### Проверка бизнес-правила "1 заказ" - "1 дата"



Проверим датасет на наличие строк, когда одному номеру заказа соответствует несколько разных дат:



```python

df_order_date_check = df.groupby(['order_id']).agg({'date' : 'nunique'}).reset_index()
df_order_date_check.columns = ['order_id', 'date_counter']

df_order_date_check = \
        df_order_date_check.query('date_counter > 1').sort_values(by='date_counter', ascending=False)
df_order_date_check.info()

orders_delete_list_2 = df_order_date_check['order_id']

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 256 entries, 221 to 2738
    Data columns (total 2 columns):
     #   Column        Non-Null Count  Dtype
    ---  ------        --------------  -----
     0   order_id      256 non-null    int64
     1   date_counter  256 non-null    int64
    dtypes: int64(2)
    memory usage: 6.0 KB
    


```python

df_order_date_check.tail(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>date_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>625</th>
      <td>68971</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1637</th>
      <td>70949</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1639</th>
      <td>70953</td>
      <td>2</td>
    </tr>
    <tr>
      <th>589</th>
      <td>68919</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2738</th>
      <td>73137</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_order_date_check.head(30)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>date_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>221</th>
      <td>14833</td>
      <td>74</td>
    </tr>
    <tr>
      <th>1642</th>
      <td>70960</td>
      <td>60</td>
    </tr>
    <tr>
      <th>725</th>
      <td>69162</td>
      <td>23</td>
    </tr>
    <tr>
      <th>1737</th>
      <td>71148</td>
      <td>16</td>
    </tr>
    <tr>
      <th>1363</th>
      <td>70419</td>
      <td>10</td>
    </tr>
    <tr>
      <th>924</th>
      <td>69527</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1888</th>
      <td>71463</td>
      <td>9</td>
    </tr>
    <tr>
      <th>1644</th>
      <td>70962</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13547</td>
      <td>7</td>
    </tr>
    <tr>
      <th>1477</th>
      <td>70620</td>
      <td>7</td>
    </tr>
    <tr>
      <th>1838</th>
      <td>71341</td>
      <td>7</td>
    </tr>
    <tr>
      <th>222</th>
      <td>14835</td>
      <td>7</td>
    </tr>
    <tr>
      <th>1002</th>
      <td>69694</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2181</th>
      <td>72066</td>
      <td>6</td>
    </tr>
    <tr>
      <th>953</th>
      <td>69598</td>
      <td>6</td>
    </tr>
    <tr>
      <th>27</th>
      <td>14521</td>
      <td>6</td>
    </tr>
    <tr>
      <th>1848</th>
      <td>71363</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2518</th>
      <td>72720</td>
      <td>5</td>
    </tr>
    <tr>
      <th>310</th>
      <td>68474</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1191</th>
      <td>70115</td>
      <td>5</td>
    </tr>
    <tr>
      <th>264</th>
      <td>14896</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1319</th>
      <td>70356</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2424</th>
      <td>72518</td>
      <td>5</td>
    </tr>
    <tr>
      <th>14</th>
      <td>14500</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1866</th>
      <td>71400</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1452</th>
      <td>70567</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2364</th>
      <td>72402</td>
      <td>4</td>
    </tr>
    <tr>
      <th>115</th>
      <td>14664</td>
      <td>4</td>
    </tr>
    <tr>
      <th>41</th>
      <td>14541</td>
      <td>4</td>
    </tr>
    <tr>
      <th>951</th>
      <td>69586</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



Проверим выборочно несколько таких заказов:


```python

df.groupby(['order_id', 'date', 'product']).agg({'quantity' : 'sum'}).query('order_id == 70115')

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th></th>
      <th>quantity</th>
    </tr>
    <tr>
      <th>order_id</th>
      <th>date</th>
      <th>product</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">70115</th>
      <th>2019020520</th>
      <th>ИСКУССТВЕННАЯ ЛИАНА ПЛЮЩ 2,4 М</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2019020523</th>
      <th>ИСКУССТВЕННАЯ ЛИАНА ПЛЮЩ 2,4 М</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2019020700</th>
      <th>ИСКУССТВЕННАЯ ЛИАНА ПЛЮЩ 2,4 М</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2019020919</th>
      <th>ИСКУССТВЕННАЯ ЛИАНА ПЛЮЩ 2,4 М</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2019021117</th>
      <th>ИСКУССТВЕННАЯ ЛИАНА ПЛЮЩ 2,4 М</th>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python

df.groupby(['order_id', 'date', 'product']).agg({'quantity' : 'sum'}).query('order_id == 73137 or order_id == 68971')

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th></th>
      <th>quantity</th>
    </tr>
    <tr>
      <th>order_id</th>
      <th>date</th>
      <th>product</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2" valign="top">68971</th>
      <th>2018111214</th>
      <th>ВЕШАЛКА НАПОЛЬНАЯ ATLANT 90Х42 СМ VATL-1 СВЕТЛАЯ СТЕПАНОВ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2018111217</th>
      <th>ВЕШАЛКА НАПОЛЬНАЯ ATLANT 90Х42 СМ VATL-1 СВЕТЛАЯ СТЕПАНОВ</th>
      <td>2</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">73137</th>
      <th>2019103009</th>
      <th>СУМКА-ТЕЛЕЖКА 2-Х КОЛЕСНАЯ GIMI ARGO СИНЯЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>2019103116</th>
      <th>СУМКА-ТЕЛЕЖКА 2-Х КОЛЕСНАЯ GIMI ARGO СИНЯЯ</th>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python

print('')
print('Всего строк в исходном датасете:', len(df))
print('Строк к удалению:', len(df) - len(df.query('order_id not in @orders_delete_list_2')))
print('% строк в удалению:', (len(df) - len(df.query('order_id not in @orders_delete_list_2')))/len(df)*100)
print('')

```

    
    Всего строк в исходном датасете: 7385
    Строк к удалению: 2384
    % строк в удалению: 32.28165199729181
    
    

Найдено 256 "проблемных" заказов, что соответствует 2384 транзакциям в датасете и составляет порядка 32.3%, почти 1/3 всех транзакций в датасете. Учитывая это, целесообразно проинформировать Заказчика о проблемах в предоставленной выгрузке с данными.  Чтобы исключить искажение результатов (смещение статистик) для дальнейшего исследования удалим такие заказы (все транзакции с этими заказами) из датасета.


```python

df.info()

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 7385 entries, 0 to 7473
    Data columns (total 6 columns):
     #   Column       Non-Null Count  Dtype  
    ---  ------       --------------  -----  
     0   date         7385 non-null   int64  
     1   customer_id  7385 non-null   object 
     2   order_id     7385 non-null   int64  
     3   product      7385 non-null   object 
     4   quantity     7385 non-null   int64  
     5   price        7385 non-null   float64
    dtypes: float64(1), int64(3), object(2)
    memory usage: 403.9+ KB
    


```python

df = df.query('order_id not in @orders_delete_list_2')
df.info()

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 5001 entries, 0 to 7473
    Data columns (total 6 columns):
     #   Column       Non-Null Count  Dtype  
    ---  ------       --------------  -----  
     0   date         5001 non-null   int64  
     1   customer_id  5001 non-null   object 
     2   order_id     5001 non-null   int64  
     3   product      5001 non-null   object 
     4   quantity     5001 non-null   int64  
     5   price        5001 non-null   float64
    dtypes: float64(1), int64(3), object(2)
    memory usage: 273.5+ KB
    


Осталось уникальных заказов в датасете:



```python

print(len(df['order_id'].unique()))

```

    3236
    

#### Преобразование типов полей и добавление дополнительных полей

Преобразуем тип столбца `date` к *datetime*:


```python

df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H')

```

Добавим дополнительные поля ('месяц', 'номер недели', 'день недели', час и т.д.) для последующего анализа:


```python

df['year']  = df['date'].dt.year
df['week'] = df['date'].dt.week
df['month']  = df['date'].dt.month
df['month_name']  = df['date'].dt.month_name()
df['day'] = df['date'].dt.day
df['week_day'] = df['date'].dt.dayofweek
df['week_day_name'] = df['date'].dt.day_name()
df['hour']  = df['date'].dt.hour

```

Выборочно проверим наборы уникальных значений:


```python

df['month'].unique()

```




    array([10, 11, 12,  1,  2,  3,  4,  5,  6,  7,  8,  9], dtype=int64)




```python

df['day'].unique()

```




    array([ 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
           18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
          dtype=int64)




```python

df['week_day'].unique()

```




    array([0, 1, 2, 3, 4, 5, 6], dtype=int64)




```python

df['hour'].unique() 

```




    array([ 0,  8,  9, 11, 12, 13, 15, 16, 10, 14, 17, 18, 21, 20,  7, 19,  3,
            5, 22, 23,  1,  2,  6,  4], dtype=int64)



Добавим дополнительное поле `total_price` (общая стоимость заказанной позиции товара: цена 1 ед. товара **х** количество товара)':


```python

df['total_price'] = df['price'] * df['quantity'] 
df['total_price'].describe()

```




    count      5001.000000
    mean        854.635156
    std        9687.472861
    min           9.000000
    25%         120.000000
    50%         194.000000
    75%         734.000000
    max      675000.000000
    Name: total_price, dtype: float64




```python

df.head(3)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
      <th>year</th>
      <th>week</th>
      <th>month</th>
      <th>month_name</th>
      <th>day</th>
      <th>week_day</th>
      <th>week_day_name</th>
      <th>hour</th>
      <th>total_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ АЛОЕ ВЕРА, D12, H30</td>
      <td>1</td>
      <td>142.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>142.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ КОФЕ АРАБИКА, D12, H25</td>
      <td>1</td>
      <td>194.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>194.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>РАДЕРМАХЕРА D-12 СМ H-20 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
    </tr>
  </tbody>
</table>
</div>




```python

df.tail(3)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
      <th>year</th>
      <th>week</th>
      <th>month</th>
      <th>month_name</th>
      <th>day</th>
      <th>week_day</th>
      <th>week_day_name</th>
      <th>hour</th>
      <th>total_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>7471</th>
      <td>2020-01-31 02:00:00</td>
      <td>2C9BD08D-8C55-4E7A-9BFB-8C56BA42C6D6</td>
      <td>106336</td>
      <td>ПОДСТАВКА ДЛЯ ОБУВИ РЕЗИНОВАЯ ATTRIBUTE 80X40 СМ AMC080</td>
      <td>1</td>
      <td>354.0</td>
      <td>2020</td>
      <td>5</td>
      <td>1</td>
      <td>January</td>
      <td>31</td>
      <td>4</td>
      <td>Friday</td>
      <td>2</td>
      <td>354.0</td>
    </tr>
    <tr>
      <th>7472</th>
      <td>2020-01-31 12:00:00</td>
      <td>CDD17932-623E-415F-A577-3B31312FD0E2</td>
      <td>102002</td>
      <td>ТАГЕТИС КРУПНОЦВЕТКОВЫЙ РАССАДА ОДНОЛЕТНИХ ЦВЕТОВ В КАССЕТЕ ПО 6 ШТ</td>
      <td>1</td>
      <td>128.0</td>
      <td>2020</td>
      <td>5</td>
      <td>1</td>
      <td>January</td>
      <td>31</td>
      <td>4</td>
      <td>Friday</td>
      <td>12</td>
      <td>128.0</td>
    </tr>
    <tr>
      <th>7473</th>
      <td>2020-01-31 15:00:00</td>
      <td>2E460A26-35AF-453D-A369-A036E95A40E0</td>
      <td>103225</td>
      <td>ВЕШАЛКА ДЛЯ БЛУЗОК 41 СМ КРАСНЫЙ ATTRIBUTE AHM781</td>
      <td>1</td>
      <td>104.0</td>
      <td>2020</td>
      <td>5</td>
      <td>1</td>
      <td>January</td>
      <td>31</td>
      <td>4</td>
      <td>Friday</td>
      <td>15</td>
      <td>104.0</td>
    </tr>
  </tbody>
</table>
</div>



#### Проверка выбросов

Проверим наличие выбросов в поле `количество` и `стоимость` товара в транзакции.


```python

df[['quantity','total_price']].describe()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>quantity</th>
      <th>total_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>5001.000000</td>
      <td>5001.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>2.558688</td>
      <td>854.635156</td>
    </tr>
    <tr>
      <th>std</th>
      <td>17.031835</td>
      <td>9687.472861</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1.000000</td>
      <td>9.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>1.000000</td>
      <td>120.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>1.000000</td>
      <td>194.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>1.000000</td>
      <td>734.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>1000.000000</td>
      <td>675000.000000</td>
    </tr>
  </tbody>
</table>
</div>



Минимальное значение  - 1, максимальное - 1000 для `quantity`, и соответственно, 9 и 675000 для `total_price`.
Построим "ящики с усами"...


```python

fig = plt.subplots(figsize=(10,3))

plt.title("Распределение количества товаров в транзакции", fontsize=12)
plot = sns.boxplot(x='quantity', data=df, color=".7")

plt.xlabel('шт', fontsize=10)
None

```


    
![png](output_79_0.png)
    



```python

fig = plt.subplots(figsize=(10,3))

plt.title("Распределение стимости товаров в заказе", fontsize=12)

plot = sns.boxplot(x='total_price',
    data=df.pivot_table(index=['order_id'], values='total_price', aggfunc='sum').reset_index(),\
                  color=".7")

plt.xlabel('продажи, млн', fontsize=10)
None

```


    
![png](output_80_0.png)
    



```python

df_q = df[['date','order_id','product','price','quantity','total_price']].sort_values('quantity', ascending=False)
df_q.head(10)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>order_id</th>
      <th>product</th>
      <th>price</th>
      <th>quantity</th>
      <th>total_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>5456</th>
      <td>2019-06-18 15:00:00</td>
      <td>71743</td>
      <td>ВАНТУЗ С ДЕРЕВЯННОЙ РУЧКОЙ D14 СМ КРАСНЫЙ, BURSTENMANN, 0522/0000</td>
      <td>675.0</td>
      <td>1000</td>
      <td>675000.0</td>
    </tr>
    <tr>
      <th>5071</th>
      <td>2019-06-11 07:00:00</td>
      <td>71668</td>
      <td>ВЕШАЛКИ МЯГКИЕ ДЛЯ ДЕЛИКАТНЫХ ВЕЩЕЙ 3 ШТ ШОКОЛАД</td>
      <td>148.0</td>
      <td>334</td>
      <td>49432.0</td>
    </tr>
    <tr>
      <th>3961</th>
      <td>2019-05-20 21:00:00</td>
      <td>71478</td>
      <td>МУЛЯЖ ЯБЛОКО 9 СМ КРАСНОЕ</td>
      <td>51.0</td>
      <td>300</td>
      <td>15300.0</td>
    </tr>
    <tr>
      <th>1158</th>
      <td>2018-12-10 14:00:00</td>
      <td>69289</td>
      <td>РУЧКА-СКОБА РС-100 БЕЛАЯ *ТРИБАТРОН*, 1108035</td>
      <td>29.0</td>
      <td>200</td>
      <td>5800.0</td>
    </tr>
    <tr>
      <th>568</th>
      <td>2018-11-01 08:00:00</td>
      <td>68815</td>
      <td>МУЛЯЖ ЯБЛОКО 9 СМ КРАСНОЕ</td>
      <td>51.0</td>
      <td>170</td>
      <td>8670.0</td>
    </tr>
    <tr>
      <th>2431</th>
      <td>2019-03-23 10:00:00</td>
      <td>70841</td>
      <td>ПЛЕЧИКИ ПЛАСТМАССОВЫЕ РАЗМЕР 52 - 54 ТУЛА 1205158</td>
      <td>20.0</td>
      <td>150</td>
      <td>3000.0</td>
    </tr>
    <tr>
      <th>586</th>
      <td>2018-11-02 11:00:00</td>
      <td>68831</td>
      <td>МУЛЯЖ ЯБЛОКО 9 СМ КРАСНОЕ</td>
      <td>59.0</td>
      <td>140</td>
      <td>8260.0</td>
    </tr>
    <tr>
      <th>1103</th>
      <td>2018-12-04 17:00:00</td>
      <td>69206</td>
      <td>ЩЕТКА ДЛЯ ПОСУДЫ *ОЛЯ*, МУЛЬТИПЛАСТ 1807010</td>
      <td>26.0</td>
      <td>100</td>
      <td>2600.0</td>
    </tr>
    <tr>
      <th>6535</th>
      <td>2019-10-07 11:00:00</td>
      <td>72885</td>
      <td>КРЕПЕЖ ДЛЯ ПРУЖИНЫ ДВЕРНОЙ ОЦИНКОВАННЫЙ, 1107054</td>
      <td>19.0</td>
      <td>100</td>
      <td>1900.0</td>
    </tr>
    <tr>
      <th>1555</th>
      <td>2019-01-21 09:00:00</td>
      <td>69893</td>
      <td>ЩЕТКА ДЛЯ МЫТЬЯ ПОСУДЫ КОЛИБРИ М5202 БОЛЬШАЯ</td>
      <td>34.0</td>
      <td>100</td>
      <td>3400.0</td>
    </tr>
  </tbody>
</table>
</div>



Высокие значения для количества товара вполне могут соответствовать большим оптовым закупкам, поэтому удалять значения из датасета не будем.

#### Категории товаров

Оценим количество уникальных товаров, представленных в ассортименте магазина:


```python

print(len(df['product'].unique()))

```

    2196
    

Учитывая, что разбивка товаров на категории с применением алгоритмов лемматизации и прочих алгоритмов требует значительных трудозатрат и выходит за рамки реализации учебного проекта, применим более простой подход к разбивке товаров на категории.

Добавим в датасет новое вспомогательное поле-подстроку, которое сформируем как первое слово из наименования позиции товара в столбце product):


```python

df['product_sstr'] = df['product'].str.split(' ').str[0]

```


```python

print(len(df['product_sstr'].unique()))

```

    435
    

При этом получаем 435 уникальных значений в этом поле. Для рейтингования позиций товаров сформируем сводную таблицу и рассмотрим эти значения:


```python

df_keyword = df.pivot_table(index=['product_sstr'], values='order_id', aggfunc='count').sort_values('order_id', ascending=False)
df_keyword

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
    </tr>
    <tr>
      <th>product_sstr</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>ПЕЛАРГОНИЯ</th>
      <td>681</td>
    </tr>
    <tr>
      <th>РАССАДА</th>
      <td>452</td>
    </tr>
    <tr>
      <th>СУШИЛКА</th>
      <td>281</td>
    </tr>
    <tr>
      <th>СУМКА-ТЕЛЕЖКА</th>
      <td>242</td>
    </tr>
    <tr>
      <th>МУЛЯЖ</th>
      <td>159</td>
    </tr>
    <tr>
      <th>ТОМАТА</th>
      <td>152</td>
    </tr>
    <tr>
      <th>ПЕТУНИЯ</th>
      <td>147</td>
    </tr>
    <tr>
      <th>ГЛАДИЛЬНАЯ</th>
      <td>126</td>
    </tr>
    <tr>
      <th>КОВРИК</th>
      <td>118</td>
    </tr>
    <tr>
      <th>ГЕРАНЬ</th>
      <td>109</td>
    </tr>
    <tr>
      <th>ЧЕХОЛ</th>
      <td>103</td>
    </tr>
    <tr>
      <th>ТЕЛЕЖКА</th>
      <td>99</td>
    </tr>
    <tr>
      <th>ШТОРА</th>
      <td>81</td>
    </tr>
    <tr>
      <th>РОЗА</th>
      <td>69</td>
    </tr>
    <tr>
      <th>ТАЗ</th>
      <td>66</td>
    </tr>
    <tr>
      <th>ОДНОЛЕТНЕЕ</th>
      <td>63</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ</th>
      <td>58</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА</th>
      <td>49</td>
    </tr>
    <tr>
      <th>НАБОР</th>
      <td>45</td>
    </tr>
    <tr>
      <th>КОРЗИНА</th>
      <td>39</td>
    </tr>
    <tr>
      <th>ЦВЕТОК</th>
      <td>39</td>
    </tr>
    <tr>
      <th>СКАТЕРТЬ</th>
      <td>38</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА</th>
      <td>36</td>
    </tr>
    <tr>
      <th>КАЛИБРАХОА</th>
      <td>35</td>
    </tr>
    <tr>
      <th>БАКОПА</th>
      <td>29</td>
    </tr>
    <tr>
      <th>САЛАТНИК</th>
      <td>28</td>
    </tr>
    <tr>
      <th>СТРЕМЯНКА</th>
      <td>24</td>
    </tr>
    <tr>
      <th>ЧАЙНИК</th>
      <td>24</td>
    </tr>
    <tr>
      <th>ПОЛКИ</th>
      <td>23</td>
    </tr>
    <tr>
      <th>ФУКСИЯ</th>
      <td>22</td>
    </tr>
    <tr>
      <th>ПОДВЕСНОЕ</th>
      <td>21</td>
    </tr>
    <tr>
      <th>БАЗИЛИК</th>
      <td>21</td>
    </tr>
    <tr>
      <th>БАНКА</th>
      <td>19</td>
    </tr>
    <tr>
      <th>НОЖ</th>
      <td>19</td>
    </tr>
    <tr>
      <th>ПРИМУЛА</th>
      <td>19</td>
    </tr>
    <tr>
      <th>КОНТЕЙНЕР</th>
      <td>19</td>
    </tr>
    <tr>
      <th>МЯТА</th>
      <td>19</td>
    </tr>
    <tr>
      <th>ФЛОКС</th>
      <td>18</td>
    </tr>
    <tr>
      <th>ЁРШ</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ДЕКАБРИСТ</th>
      <td>16</td>
    </tr>
    <tr>
      <th>КРУЖКА</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ФИАЛКА</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННАЯ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ЩЕТКА</th>
      <td>15</td>
    </tr>
    <tr>
      <th>БЕГОНИЯ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ШВАБРА</th>
      <td>14</td>
    </tr>
    <tr>
      <th>АНТУРИУМ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ПУАНСЕТТИЯ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ЦИКЛАМЕН</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ЛЕСТНИЦА-СТРЕМЯНКА</th>
      <td>13</td>
    </tr>
    <tr>
      <th>КАРНИЗ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ЛОБЕЛИЯ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>КАПУСТА</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ВЕРБЕНА</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ПОДКЛАДКА</th>
      <td>12</td>
    </tr>
    <tr>
      <th>НОВОГОДНЕЕ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ДЕКОРАТИВНАЯ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ГВОЗДИКА</th>
      <td>11</td>
    </tr>
    <tr>
      <th>САЛФЕТКА</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ТИМЬЯН</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ВЕДРО</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ПОДРУКАВНИК</th>
      <td>10</td>
    </tr>
    <tr>
      <th>КОСМЕЯ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СУМКА</th>
      <td>10</td>
    </tr>
    <tr>
      <th>КОМПЛЕКТ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СИДЕНЬЕ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ТЕРМОКРУЖКА</th>
      <td>10</td>
    </tr>
    <tr>
      <th>КОМНАТНОЕ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ОКНОМОЙКА</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ЭВКАЛИПТ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ЦИПЕРУС</th>
      <td>9</td>
    </tr>
    <tr>
      <th>КОВЕР</th>
      <td>9</td>
    </tr>
    <tr>
      <th>ШТАНГА</th>
      <td>9</td>
    </tr>
    <tr>
      <th>ЩЕТКА-СМЕТКА</th>
      <td>9</td>
    </tr>
    <tr>
      <th>ПЛЕЧИКИ</th>
      <td>9</td>
    </tr>
    <tr>
      <th>БАЛЬЗАМИН</th>
      <td>9</td>
    </tr>
    <tr>
      <th>ВЕСЫ</th>
      <td>9</td>
    </tr>
    <tr>
      <th>ПЕРЧАТКИ</th>
      <td>9</td>
    </tr>
    <tr>
      <th>ЛОЖКА</th>
      <td>9</td>
    </tr>
    <tr>
      <th>СКОВОРОДА</th>
      <td>9</td>
    </tr>
    <tr>
      <th>КРЮЧОК</th>
      <td>8</td>
    </tr>
    <tr>
      <th>ДЫНЯ</th>
      <td>8</td>
    </tr>
    <tr>
      <th>ВИОЛА</th>
      <td>8</td>
    </tr>
    <tr>
      <th>ГАЗАНИЯ</th>
      <td>8</td>
    </tr>
    <tr>
      <th>ПОДСТАВКА</th>
      <td>8</td>
    </tr>
    <tr>
      <th>КУВШИН</th>
      <td>8</td>
    </tr>
    <tr>
      <th>ХРИЗАНТЕМА</th>
      <td>8</td>
    </tr>
    <tr>
      <th>НАСТУРЦИЯ</th>
      <td>8</td>
    </tr>
    <tr>
      <th>КАСТРЮЛЯ</th>
      <td>7</td>
    </tr>
    <tr>
      <th>ХЛЕБНИЦА</th>
      <td>7</td>
    </tr>
    <tr>
      <th>ТОМАТ</th>
      <td>7</td>
    </tr>
    <tr>
      <th>ПОДАРОЧНЫЙ</th>
      <td>7</td>
    </tr>
    <tr>
      <th>ПЕТРУШКА</th>
      <td>7</td>
    </tr>
    <tr>
      <th>ЯЩИК</th>
      <td>7</td>
    </tr>
    <tr>
      <th>ВИЛКА</th>
      <td>7</td>
    </tr>
    <tr>
      <th>АФЕЛЯНДРА</th>
      <td>7</td>
    </tr>
    <tr>
      <th>РУКАВ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>КАЛАТЕЯ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>КОЛОКОЛЬЧИК</th>
      <td>6</td>
    </tr>
    <tr>
      <th>КОФР</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ЛАВАНДА</th>
      <td>6</td>
    </tr>
    <tr>
      <th>МИСКА</th>
      <td>6</td>
    </tr>
    <tr>
      <th>НАСАДКА</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ОГУРЕЦ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>СРЕДСТВО</th>
      <td>6</td>
    </tr>
    <tr>
      <th>СТАКАН</th>
      <td>6</td>
    </tr>
    <tr>
      <th>КОРЫТО</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ЦИННИЯ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ЭХЕВЕРИЯ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ТЕРМОС</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ЭТАЖЕРКА</th>
      <td>6</td>
    </tr>
    <tr>
      <th>АЗАЛИЯ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>ХЛОРОФИТУМ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>АРБУЗ</th>
      <td>6</td>
    </tr>
    <tr>
      <th>МУСОРНЫЙ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ЧАЙНЫЙ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ОВОЩЕВАРКА</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ШНУР</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ТЕРМОМЕТР</th>
      <td>5</td>
    </tr>
    <tr>
      <th>АЛИССУМ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>МИРТ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ЛЬВИНЫЙ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ПЕТЛЯ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ЛОТОК</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ВЕШАЛКИ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ТЕРКА</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ПОДГОЛОВНИК</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ПОКРЫВАЛО</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ПОКРЫТИЕ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ПОЛОТЕНЦЕ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ВЕНИК</th>
      <td>5</td>
    </tr>
    <tr>
      <th>РАЗДЕЛОЧНАЯ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ДЕНДРОБИУМ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>СТРЕМЯНКИ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>СТЯЖКА</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ТАБАК</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ЖЕСТЯНАЯ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>TEPМОКРУЖКА</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ЗВЕРОБОЙ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>ЗУБНАЯ</th>
      <td>5</td>
    </tr>
    <tr>
      <th>КОМОД</th>
      <td>4</td>
    </tr>
    <tr>
      <th>СЕТКА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ТРЯПКОДЕРЖАТЕЛЬ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ЛЕВКОЙ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>АСТРА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>МЫЛО</th>
      <td>4</td>
    </tr>
    <tr>
      <th>БЕНЗИН</th>
      <td>4</td>
    </tr>
    <tr>
      <th>КАЛЕНДУЛА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>СТОЛОВАЯ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>СПАТИФИЛЛУМ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>СОВОК</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ПРОСЕИВАТЕЛЬ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ВАНТУЗ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ЛАВР</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ПОРТУЛАК</th>
      <td>4</td>
    </tr>
    <tr>
      <th>МЕДИНИЛЛА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА-ПЛЕЧИКИ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>МИКСЕР</th>
      <td>4</td>
    </tr>
    <tr>
      <th>МИМОЗА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ГАРДЕНИЯ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ОДЕЯЛО</th>
      <td>4</td>
    </tr>
    <tr>
      <th>НЕЗАБУДКА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>УКРОП</th>
      <td>4</td>
    </tr>
    <tr>
      <th>НАСТЕННАЯ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>УНИВЕРСАЛЬНОЕ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ФОРМА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ЭХИНОКАКТУС</th>
      <td>4</td>
    </tr>
    <tr>
      <th>КОРЕОПСИС</th>
      <td>4</td>
    </tr>
    <tr>
      <th>КОРОБКА</th>
      <td>4</td>
    </tr>
    <tr>
      <th>КОТОВНИК</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ЦВЕТУЩЕЕ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>АПТЕНИЯ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ИЗМЕЛЬЧИТЕЛЬ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>КОФЕ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>ГОРТЕНЗИЯ</th>
      <td>4</td>
    </tr>
    <tr>
      <th>АРОМАТИЗИРОВАННОЕ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ФАЛ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ГЕРБЕРА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>КЛЕН</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ОВСЯННИЦА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>АКВИЛЕГИЯ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ФАЛЕНОПСИС</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ПАСТА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ПЛЕД</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА-СУШИЛКА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ШТАНГЕНЦИРКУЛЬ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ЗЕМЛЯНИКА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА-СТОЙКА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ВЕРБЕЙНИК</th>
      <td>3</td>
    </tr>
    <tr>
      <th>СТЕЛЛАЖ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ШЕФФЛЕРА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ЧАЙНАЯ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ТОЛКУШКА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ПРОСТЫНЯ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>УРНА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>РОЗМАРИН</th>
      <td>3</td>
    </tr>
    <tr>
      <th>РЫБОЧИСТКА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>СЕДУМ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>АСПАРАГУС</th>
      <td>3</td>
    </tr>
    <tr>
      <th>СКРЕБОК</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ФИЗОСТЕГИЯ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>КАЛАНХОЕ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ТКАНЬ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ПОДУШКА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ДИФФЕНБАХИЯ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ЛИНЕЙКА,</th>
      <td>3</td>
    </tr>
    <tr>
      <th>КОВШ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>КОВЁР</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ЛЕСТНИЦА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>МАХРОВОЕ</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ГИПСОФИЛА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ЛАНТАНА</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ДОЗАТОР</th>
      <td>3</td>
    </tr>
    <tr>
      <th>МОЛОДИЛО</th>
      <td>3</td>
    </tr>
    <tr>
      <th>ХАМЕДОРЕЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>АРГИРАНТЕРУМ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ХАЛАТ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ИЗМЕРИТЕЛЬНЫЙ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>РУЧКА-СКОБА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>САЛАТ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ВАКУУМНЫЙ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>САЛФЕТНИЦА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>САЛЬВИЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>МАСЛЕНКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПЫЛЕСОС</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КАМНЕЛОМКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПЬЕЗОЗАЖИГАЛКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>САХАРНИЦА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КАМПАНУЛА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПУАНСЕТИЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КРЕПЕЖ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ХРИЗОЛИДОКАРПУС</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПРОСТЫНЬ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПРОБКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПРИЩЕПКИ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЦЕЛОЗИЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЦИКЛАМЕН,</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КРЫШКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КЛЮЧНИЦА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СВЕРЛО-ФРЕЗА,</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ФАТСИЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>БЕЛЬЕВЫЕ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>БИДОН</th>
      <td>2</td>
    </tr>
    <tr>
      <th>БЛЮДО</th>
      <td>2</td>
    </tr>
    <tr>
      <th>БЛЮДЦЕ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>БАРХАТЦЫ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ИССОП</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЕМКОСТЬ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЕРШ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ТЕРМОСТАКАН</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СТОЛОВЫЙ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>БАК</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СТЕКЛЯННАЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЛОПАТКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СОТЕЙНИК</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СОЛАНУМ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СОКОВЫЖИМАЛКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СОКОВАРКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЛАПЧАТКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ТЮЛЬПАН</th>
      <td>2</td>
    </tr>
    <tr>
      <th>УВЛАЖНЯЮЩАЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>УНИВЕРСАЛЬНЫЙ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>УТЮГ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СИТО</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПОЛКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>СЕРВИРОВОЧНАЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЦИНЕРАНИЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КРАССУЛА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ДУШИЦА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>АЛОЭ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>МНОГОЛЕТНЕЕ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КОЛЬЦА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КОРИАНДР</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ОТДЕЛИТЕЛЬ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ШПАГАТ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЦИНЕРАРИЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ОСИНА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ОРЕХОКОЛКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ДЕВИЧИЙ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>МОДУЛЬНАЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КИСТОЧКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>МОНАРДА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>НОЛИНА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>АВТОМАТИЧЕСКАЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ГИАЦИНТ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>НИВЯННИК</th>
      <td>2</td>
    </tr>
    <tr>
      <th>НЕФРОЛЕПИС</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЭНОТЕРА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>МУРРАЙЯ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>НАСАДКА-МОП</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КЛУБНИКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ГИНОСТЕММА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ГИПОАЛЛЕРГЕННЫЙ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПАТИССОН</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ТАГЕТИС</th>
      <td>2</td>
    </tr>
    <tr>
      <th>КИПАРИСОВИК</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПИРЕТРУМ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЗМЕЕГОЛОВНИК</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ВЕРОНИКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ВКЛАДЫШИ</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЦИТРОФОРТУНЕЛЛА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ДОСКА</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЧАБЕР</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ПЛАТИКОДОН</th>
      <td>2</td>
    </tr>
    <tr>
      <th>МЕШОК</th>
      <td>2</td>
    </tr>
    <tr>
      <th>ЛИЛЕЙНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЗАПАСНАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ИМПАТИЕНС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ТРЯПКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГОРОХ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЭПИПРЕМНУМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЛЕН</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ФИТТОНИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>АДИАНТУМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>АНЕМОНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЗАМИОКУЛЬКАС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КОТЕЛ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>БАЛЬЗАМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КОЛЕУС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ДЖУНКУС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЭШШОЛЬЦИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЮККА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЦИНИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>АНТИЖИР</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЯСКОЛКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ДВУСПАЛЬНОЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЩЁТКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>АМАРИЛЛИС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЗАЩИТНЫЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ФИКУС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>АРТЕМИЗИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ФИКСАТОР-ШАР</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГУБКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ФОТОРАМКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КРОКУСЫ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЧИСТЯЩИЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>АЛЬБУКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГОТОВАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ФЕН</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ШПИНГАЛЕТ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КУХОННОЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КУХОННЫЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КОРЗИНКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ХОЛОДНАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>УРНА-ПЕПЕЛЬНИЦА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КОДОНАНТА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КОНЦЕНТРАТ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЛАВАТЕРА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>УГОЛОК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ШАЛФЕЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ФАРФОРОВАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>БУЗУЛЬНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СЦИНДАПСУС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПАХИРА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПЕНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПЕПЕРОМИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КИПЯТИЛЬНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МЕРНЫЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КАРТОФЕЛЕМЯЛКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА-ПЕРЕКЛАДИНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПОДОДЕЯЛЬНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПОДСОЛНЕЧНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ВЕРЕВКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЖИДКОЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ДРАЦЕНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МЕЛИССА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КАПСИКУМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МАХРОВЫЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПРЕСС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ВЕНЧИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МАТТИОЛА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГАЙЛАРДИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПАПОРОТНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПРЯНЫЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ОТЖИМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МЫЛЬНИЦА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НАВОЛОЧКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НАМАТРАСНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НАМАТРАЦНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НАСАДКА-ОТЖИМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НАСТОЛЬНАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГИМНОКАЛИЦИУМ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НЕТКАНЫЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МОРКОВЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>НОЖЕТОЧКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ОБУВНИЦА-3</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ОВОЩЕЧИСТКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГЕОРГИНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МНОГОФУНКЦИОНАЛЬНЫЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ОПОЛАСКИВАТЕЛЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ОСНОВАНИЕ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ОТБЕЛИВАТЕЛЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ПРОТИВЕНЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РАДЕРМАХЕРА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СУККУЛЕНТ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СКЛАДНАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МЫЛО-СКРАБ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЛУК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СМЕННАЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СМЕННЫЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СМЕТКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СМОЛЕВКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СОЛИДАГО</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КАЛЛУНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КАЛЛА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГЛОКСИНИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СТИРАЛЬНЫЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ГОДЕЦИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>БУДДЛЕЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СТРЕМЯНКА-ТАБУРЕТ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЕЛЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЛИТОПС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СТЯЖКИ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СКЛАДНОЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СКИММИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ДЕРЖАТЕЛЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>БУЛЬОННИЦА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РАССЕКАТЕЛЬ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РЕШЕТКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ВАННА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МАНТОВАРКА-ПАРОВАРКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РОЛИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РУДБЕКИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>КАЛОЦЕФАЛУС</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РУЧКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>МАНТОВАРКА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ВАЛЕРИАНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>САНТОЛИНА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СВЕРЛО</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СВЕТИЛЬНИК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ЛЮБИСТОК</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СЕЛЬДЕРЕЙ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>ВАЗА</th>
      <td>1</td>
    </tr>
    <tr>
      <th>СИННИНГИЯ</th>
      <td>1</td>
    </tr>
    <tr>
      <th>РАНУНКУЛУС</th>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



А также при формирования словаря для категорий товаров будем учитывать рейтинг по количеству товара.


```python

df_q = df.groupby('product').agg({'quantity' : 'sum'}).sort_values(by='quantity', ascending=False)
df_q.head(200)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>quantity</th>
    </tr>
    <tr>
      <th>product</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>ВАНТУЗ С ДЕРЕВЯННОЙ РУЧКОЙ D14 СМ КРАСНЫЙ, BURSTENMANN, 0522/0000</th>
      <td>1000</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ЯБЛОКО 9 СМ КРАСНОЕ</th>
      <td>618</td>
    </tr>
    <tr>
      <th>ВЕШАЛКИ МЯГКИЕ ДЛЯ ДЕЛИКАТНЫХ ВЕЩЕЙ 3 ШТ ШОКОЛАД</th>
      <td>335</td>
    </tr>
    <tr>
      <th>РУЧКА-СКОБА РС-100 БЕЛАЯ *ТРИБАТРОН*, 1108035</th>
      <td>201</td>
    </tr>
    <tr>
      <th>ПЛЕЧИКИ ПЛАСТМАССОВЫЕ РАЗМЕР 52 - 54 ТУЛА 1205158</th>
      <td>160</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ЯБЛОКО ЗЕЛЕНЫЙ 9 СМ ПОЛИУРЕТАН</th>
      <td>148</td>
    </tr>
    <tr>
      <th>МУЛЯЖ БАНАН ЖЕЛТЫЙ 21 СМ ПОЛИУРЕТАН</th>
      <td>108</td>
    </tr>
    <tr>
      <th>ЩЕТКА-СМЕТКА 4-Х РЯДНАЯ ДЕРЕВЯННАЯ 300 ММ (ФИГУРНАЯ РУЧКА) ВОРС 5,5 СМ 1801096</th>
      <td>105</td>
    </tr>
    <tr>
      <th>ЩЕТКА ДЛЯ ПОСУДЫ *ОЛЯ*, МУЛЬТИПЛАСТ 1807010</th>
      <td>101</td>
    </tr>
    <tr>
      <th>ШПИНГАЛЕТ 80 ММ БЕЛЫЙ С ПРУЖИНОЙ, 1102188</th>
      <td>100</td>
    </tr>
    <tr>
      <th>ЩЕТКА ДЛЯ МЫТЬЯ ПОСУДЫ КОЛИБРИ М5202 БОЛЬШАЯ</th>
      <td>100</td>
    </tr>
    <tr>
      <th>КРЕПЕЖ ДЛЯ ПРУЖИНЫ ДВЕРНОЙ ОЦИНКОВАННЫЙ, 1107054</th>
      <td>100</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ ГВОЗДИКА ПЛАСТИКОВАЯ ОДИНОЧНАЯ В АССОРТИМЕНТЕ 50 СМ</th>
      <td>97</td>
    </tr>
    <tr>
      <th>КРЮЧОК ОДЕЖНЫЙ 2-Х РОЖКОВЫЙ СЕРЫЙ МЕТАЛЛИК С ПОЛИМЕРНЫМ ПОКРЫТИЕМ *ТОНАР*, 1110027</th>
      <td>96</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ЛИМОН ЖЕЛТЫЙ 9 СМ ПОЛИУРЕТАН</th>
      <td>92</td>
    </tr>
    <tr>
      <th>ПЕТЛЯ ПРИВАРНАЯ ГАРАЖНАЯ D 14Х90 ММ С ШАРОМ, 1103003</th>
      <td>90</td>
    </tr>
    <tr>
      <th>СТЯЖКА ОКОННАЯ С БОЛТОМ СТ-55 ЦИНК, 1108354</th>
      <td>89</td>
    </tr>
    <tr>
      <th>ЁРШ УНИТАЗНЫЙ С ДЕРЕВЯННОЙ РУЧКОЙ , ВАИР 1712012</th>
      <td>83</td>
    </tr>
    <tr>
      <th>УНИВЕРСАЛЬНЫЙ НОЖ WEBBER ИЗ НЕРЖАВЕЮЩЕЙ СТАЛИ РУССКИЕ МОТИВЫ 12,7 СМ С ДЕРЕВЯННОЙ РУЧКОЙ ВЕ-2252D</th>
      <td>81</td>
    </tr>
    <tr>
      <th>ОДНОЛЕТНЕЕ РАСТЕНИЕ ПЕТУНИЯ МАХРОВАЯ В КАССЕТЕ 4 ШТ, РОССИЯ</th>
      <td>76</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗЕБУДНАЯ RED PANDORA УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>74</td>
    </tr>
    <tr>
      <th>НАСАДКА НА ВАЛИК ВММ-200/60 БЕЛЫЙ ИСКУССТВЕННЫЙ МЕХ, 0703011</th>
      <td>70</td>
    </tr>
    <tr>
      <th>КОВШ ПЛАСТМАССОВЫЙ ПОЛИМЕРБЫТ С215, 1,5 Л 1406006</th>
      <td>69</td>
    </tr>
    <tr>
      <th>САЛФЕТКА PROTEC TEXTIL POLYLINE 30Х43 СМ АМЕТИСТ БЕЛАЯ 6230</th>
      <td>66</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ПОДСНЕЖНИК САДОВЫЙ ПЛАСТИКОВЫЙ БЕЛЫЙ</th>
      <td>66</td>
    </tr>
    <tr>
      <th>НАБОР НОЖЕЙ ATTRIBUTE CHEF 5 ПРЕДМЕТОВ AKF522</th>
      <td>64</td>
    </tr>
    <tr>
      <th>КРУЖКА С ТРУБОЧКОЙ ATTRIBUTE МЯЧ 500 МЛ JAR501</th>
      <td>61</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА СУПОВАЯ LUMINARC ОКЕАН ЭКЛИПС 20,5 СМ L5079</th>
      <td>60</td>
    </tr>
    <tr>
      <th>ТЕЛЕЖКА БАГАЖНАЯ DELTA ТБР-22 СИНИЙ ГРУЗОПОДЪЕМНОСТЬ 20 КГ СУМКА И 50 КГ КАРКАС РОССИЯ</th>
      <td>59</td>
    </tr>
    <tr>
      <th>МУЛЯЖ КРАСНОЕ ЯБЛОКО МИНИ ПОЛИУРЕТАН D-6 СМ</th>
      <td>59</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ СИРЕНЕВЫЙ ПОЛУМАХРОВЫЙ</th>
      <td>58</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ КРАСНАЯ МАХРОВАЯ</th>
      <td>57</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗЕБУДНАЯ PRINS NIKOLAI УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>56</td>
    </tr>
    <tr>
      <th>КОВРИК ПРИДВЕРНЫЙ ATTRIBUTE NATURE КОКОСОВЫЙ 60X40 СМ AMC015</th>
      <td>55</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ОРАНЖЕВОЕ ЯБЛОКО МИНИ ПОЛИУРЕТАН D-5 СМ</th>
      <td>54</td>
    </tr>
    <tr>
      <th>ВЕДРО РЕЗИНОПЛАСТИКОВОЕ СТРОИТЕЛЬНОЕ 12,0 Л (МП), 1402018</th>
      <td>50</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА VALIANT ДЛЯ БРЮК И ЮБОК МЕТАЛЛИЧЕСКАЯ 30*10.5 СМ 121B11</th>
      <td>50</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ ГВОЗДИКА ТКАНЕВАЯ КРАСНАЯ 50 СМ</th>
      <td>49</td>
    </tr>
    <tr>
      <th>СПАТИФИЛЛУМ ШОПЕН D-12 СМ</th>
      <td>46</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗЕБУДНАЯ MARY УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>46</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ЯБЛОКО, ОРАНЖЕВЫЙ, 8,5 СМ, ПОЛИУРЕТАН</th>
      <td>46</td>
    </tr>
    <tr>
      <th>ГВОЗДИКА СТАНДАРТНАЯ БЕЛАЯ 60 СМ КОЛУМБИЯ ПЛАНТАЦИЯ GEOFLORA S.A.S. 25 ШТУК В УПАКОВКЕ</th>
      <td>45</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК РОЗА ЗАКРЫТАЯ МАЛАЯ ПЛАСТИКОВАЯ БЕЛАЯ</th>
      <td>45</td>
    </tr>
    <tr>
      <th>СУМКА-ТЕЛЕЖКА 2-Х КОЛЕСНАЯ GIMI ARGO СИНЯЯ</th>
      <td>39</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ МАХРОВАЯ ЛОСОСЕВАЯ</th>
      <td>39</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ КРОКУС 20 СМ</th>
      <td>38</td>
    </tr>
    <tr>
      <th>ВЕНИК СОРГО С ДЕРЕВЯННОЙ РУЧКОЙ С 4-МЯ ШВАМИ, ROZENBAL, R206204</th>
      <td>38</td>
    </tr>
    <tr>
      <th>НАБОР ВЕШАЛОК ДЛЯ КОСТЮМА 45СМ 4ШТ ЦВЕТ: КРЕМОВЫЙ, ATTRIBUTE, AHP224</th>
      <td>37</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗЕБУДНАЯ QUEEN INGRID УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>37</td>
    </tr>
    <tr>
      <th>ЦИПЕРУС ЗУМУЛА D-12 СМ H-25 СМ</th>
      <td>36</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗЕБУДНАЯ MARGARETHA УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>34</td>
    </tr>
    <tr>
      <th>ЭХИНОКАКТУС ГРУЗОНИ D-5 СМ</th>
      <td>31</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЕРЕВЯННАЯ ДЛЯ ВЕРХНЕЙ ОДЕЖДЫ 45 СМ</th>
      <td>31</td>
    </tr>
    <tr>
      <th>КРУЖКА НОРДИК 380МЛ ПРОЗРАЧНАЯ H8502 LUMINARC ФРАНЦИЯ</th>
      <td>30</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ОБЕДЕННАЯ LUMINARC ОКЕАН ЭКЛИПС 24 СМ L5078</th>
      <td>30</td>
    </tr>
    <tr>
      <th>ПРОСТЫНЬ ВАФЕЛЬНАЯ 200Х180 СМ WELLNESS RW180-01 100% ХЛОПОК</th>
      <td>30</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ НАРЦИСС ОДИНОЧНЫЙ В АССОРТИМЕНТЕ 35 СМ</th>
      <td>27</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ КОРАЛЛОВАЯ ПОЛУМАХРОВАЯ</th>
      <td>27</td>
    </tr>
    <tr>
      <th>НАБОР ВЕШАЛОК ДЛЯ КОСТЮМА 45 СМ 4 ШТ ЦВЕТ КРЕМОВЫЙ ATTRIBUTE AHP224</th>
      <td>26</td>
    </tr>
    <tr>
      <th>ГЕРАНЬ ДОМАШНЯЯ (ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ) ЦВЕТУЩАЯ D12, H25-30, КОРАЛЛОВАЯ, ПОЛУМАХРОВАЯ</th>
      <td>26</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ РИНГО ВАЙТ D-7 СМ H-10 СМ УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>26</td>
    </tr>
    <tr>
      <th>ДЕКОРАТИВНАЯ КОМПОЗИЦИЯ ИСКУСТВЕННЫХ ЦВЕТОВ БУКЕТ РОЗ ТРИ ЦВЕТКА, ЦВЕТ В АССОРТИМЕНТЕ 105 СМ</th>
      <td>26</td>
    </tr>
    <tr>
      <th>РАССАДА АРБУЗА СОРТ ОГОНЕК ГОРШОК 9Х9 СМ P-9</th>
      <td>26</td>
    </tr>
    <tr>
      <th>ГВОЗДИКА СТАНДАРТНАЯ НОВИЯ 70 СМ КОЛУМБИЯ ПЛАНТАЦИЯ TURFLOR S.A.S 25 ШТУК В УПАКОВКЕ</th>
      <td>25</td>
    </tr>
    <tr>
      <th>МУЛЯЖ АПЕЛЬСИН 8 СМ ПОЛИУРЕТАН</th>
      <td>25</td>
    </tr>
    <tr>
      <th>ЧЕХОЛ ДЛЯ ОДЕЖДЫ ОБЪЕМНЫЙ HAUSMANN С ОВАЛЬНЫМ ОКНОМ ПВХ И РУЧКАМИ 60Х140Х10 СМ ЧЕРНЫЙ HM-701403AG</th>
      <td>25</td>
    </tr>
    <tr>
      <th>РОЗА КУСТОВАЯ РЕД ЛЕЙС 60 СМ КЕНИЯ ПЛАНТАЦИЯ WARIDI LIMITED 10 ШТУК В УПАКОВКЕ</th>
      <td>25</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЛЯ КОСТЮМА ПРЯМАЯ 44 СМ КРАСНОЕ ДЕРЕВО ATTRIBUTE AHR141</th>
      <td>25</td>
    </tr>
    <tr>
      <th>РОЗА ОДНОГОЛОВАЯ ТОФФИ 70 СМ ЭКВАДОР ПЛАНТАЦИЯ GREENROSE 25 ШТУК В УПАКОВКЕ</th>
      <td>25</td>
    </tr>
    <tr>
      <th>КАРНИЗ ДЛЯ ВАННОЙ КОМНАТЫ ВИОЛЕТ РАЗДВИЖНОЙ 280 СМ С КОЛЬЦАМИ БЕЛЫЙ 2810/6</th>
      <td>25</td>
    </tr>
    <tr>
      <th>КРЮЧОК ПРОВОЛОЧНЫЙ 75 ММ БЕЛЫЙ, 1110137</th>
      <td>25</td>
    </tr>
    <tr>
      <th>КРЮЧОК ОДЕЖНЫЙ ТРОЙНОЙ ЛАТУНЬ (Б-47), 1110182</th>
      <td>25</td>
    </tr>
    <tr>
      <th>УГОЛОК ОКОННЫЙ 100Х100 ММ МЕДЬ АНТИК С ПОЛИМЕРНЫМ ПОКРЫТИЕМ *ТОНАР*, 1109038</th>
      <td>24</td>
    </tr>
    <tr>
      <th>ПЕТУНИЯ МАХРОВАЯ РАССАДА ОДНОЛЕТНИХ ЦВЕТОВ В КАССЕТЕ ПО 10 ШТ</th>
      <td>24</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ БЕЛАЯ МАХРОВАЯ</th>
      <td>24</td>
    </tr>
    <tr>
      <th>НОЖ КУХОННЫЙ УНИВЕРСАЛЬНЫЙ С ПЛАСТМАССОВОЙ РУЧКОЙ 285 Х 150 ММ, "РУССКАЯ ЛИНИЯ" (НХ-38М) МЕТИЗ 1519022</th>
      <td>24</td>
    </tr>
    <tr>
      <th>МУЛЯЖ БАКЛАЖАН 18 СМ</th>
      <td>23</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ПЕРСИКИ МИНИ ПОЛИУРЕТАН D-6 СМ</th>
      <td>23</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА С ЗАКРУГЛЕННЫМИ ПЛЕЧИКАМИ ПЕРЕКЛАДИНОЙ И КРЮЧКАМИ С ПРОТИВОСКОЛЬЗЯЩИМ ПОКРЫТИЕМ VALIANT 213R11 ЧЕРНЫЙ</th>
      <td>23</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ ЛОСОСЕВАЯ МАХРОВАЯ</th>
      <td>23</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ ТЕМНОРОЗОВАЯ ПОЛУМАХРОВАЯ</th>
      <td>22</td>
    </tr>
    <tr>
      <th>ТОМАТА (ПОМИДОР) СОРТ БЫЧЬЕ СЕРДЦЕ №14</th>
      <td>22</td>
    </tr>
    <tr>
      <th>ТАЗ ПЛАСТМАССОВЫЙ 6,0 Л ПИЩЕВОЙ М2511, 1404015</th>
      <td>22</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗЕБУДНАЯ ROSEBUD RED D-7 СМ</th>
      <td>22</td>
    </tr>
    <tr>
      <th>РАССАДА АРБУЗА СОРТ ШУГА БЭБИ ГОРШОК 9Х9 СМ P-9</th>
      <td>21</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННАЯ КОМПОЗИЦИЯ ИЗ ЦВЕТОВ ПЛАСТИКОВАЯ РОМАШКА МИНИ РОЗОВАЯ</th>
      <td>21</td>
    </tr>
    <tr>
      <th>МУЛЯЖ КРАСНОЕ ЯБЛОКО МИНИ ПОЛИУРЕТАН D-5 СМ</th>
      <td>21</td>
    </tr>
    <tr>
      <th>ТАЗ ПЛАСТМАССОВЫЙ 4,5 Л ПИЩЕВОЙ "КОСМЕЯ" (АНГОРА) 1404092</th>
      <td>21</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ТЮЛЬПАНОВИДНАЯ EMMA</th>
      <td>21</td>
    </tr>
    <tr>
      <th>КРЕПЕЖ ДЛЯ ПРУЖИНЫ ДВЕРНОЙ, 1107055</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ TOSCANA ANGELEYES BICOLOR УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ РОЗА С ЗАКРЫТЫМ БУТОНОМ ЦВЕТ В АССОРТИМЕНТЕ 50 СМ</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ТАЗ ПЛАСТМАССОВЫЙ 8,0 Л ПИЩЕВОЙ (МИНЕРАЛЬНЫЕ ВОДЫ), 1404009</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ДЕСЕРТНАЯ ATTRIBUTE ROSETTE 19 СМ ADR131</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ПЕТУНИЯ МАХРОВАЯ РАССАДА ОДНОЛЕТНИХ ЦВЕТОВ В КАССЕТЕ ПО 6 ШТ</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЛЯ БЛУЗОК 41 СМ ЗЕЛЕНЫЙ ATTRIBUTE AHM751</th>
      <td>20</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ TOSCANA ANGELEYES AMARILLO BURGUNDY УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>19</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ЖЕЛУДЬ 2 ШТ</th>
      <td>19</td>
    </tr>
    <tr>
      <th>ДЕКОРАТИВНАЯ КОМПОЗИЦИЯ ИСКУСТВЕННЫХ ЦВЕТОВ БУКЕТ РОЗ ТРИ ЦВЕТКА, ЦВЕТ В АССОРТИМЕНТЕ 85 СМ</th>
      <td>19</td>
    </tr>
    <tr>
      <th>СУШИЛКА ДЛЯ БЕЛЬЯ НАСТЕННАЯ ZALGER PRIMA 510-720 ВЕРЕВОЧНАЯ 7 ЛИНИЙ 25 М</th>
      <td>19</td>
    </tr>
    <tr>
      <th>ГЕРАНЬ ДОМАШНЯЯ (ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ) ЦВЕТУЩАЯ D12, H25-30, СИРЕНЕВЫЙ, ПРОСТАЯ</th>
      <td>18</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК КРОКУС ПЛАСТИКОВЫЙ РОЗОВЫЙ</th>
      <td>18</td>
    </tr>
    <tr>
      <th>АРБУЗ ВОЛГОГРАДЕЦ Р-9</th>
      <td>18</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ СИРЕНЕВЫЙ ПРОСТАЯ</th>
      <td>17</td>
    </tr>
    <tr>
      <th>МУЛЯЖ МОРКОВЬ 16 СМ</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА-ПЛЕЧИКИ БЕЗ ЭССЕНЦИИ ЦВЕТ ТЁМНО-РОЗОВЫЙ FWM-002/QUARTZ PINK</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ГЛАДИЛЬНАЯ ДОСКА НИКА ДСП ЭКОНОМ 106,5Х29 ЭК1</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ШТАНГЕНЦИРКУЛЬ 150 ММ ПЛАСТМАССОВЫЙ, ТОЧНОСТЬ 0,1 ММ 3015515, 0910007</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ТЕЛЕЖКА БАГАЖНАЯ DELTA ТБР-20 СИНИЙ ГРУЗОПОДЪЕМНОСТЬ 25 КГ СУМКА И 50 КГ КАРКАС РОССИЯ</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ ВАСИЛЕК 3 ЦВЕТКА 55 СМ ПЛАСТИК</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ТЕЛЕЖКА БАГАЖНАЯ DELTA ТБР-20 КОРИЧНЕВЫЙ С ОРАНЖЕВЫМ ГРУЗОПОДЪЕМНОСТЬ 25 КГ СУМКА И 50 КГ КАРКАС РОССИЯ</th>
      <td>17</td>
    </tr>
    <tr>
      <th>ТОМАТА (ПОМИДОР) ЭТУАЛЬ №85 СОРТ ДЕТЕРМИНАНТНЫЙ СРЕДНЕСПЕЛЫЙ РОЗОВЫЙ</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ЩЕТКА-СМЕТКА 3-Х РЯДНАЯ ДЕРЕВЯННАЯ 450 ММ (ПЛОСКАЯ РУЧКА), ПОИСК РИФ 1801095</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ОБЕДЕННАЯ LUMINARC ГРИН ФОРЕСТ 26 СМ H7239</th>
      <td>16</td>
    </tr>
    <tr>
      <th>СЕРВИРОВОЧНАЯ САЛФЕТКА ПВХ 26Х41 СМ ВАНДА 6001</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ЧЕХОЛ ДЛЯ ГЛАДИЛЬНОЙ ДОСКИ ATTRIBUTE METAL 140Х60 СМ ABM106</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ЛАНДЫШ ПЛАСТИКОВЫЙ МАЛЫЙ БЕЛЫЙ</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ РОЗОЦВЕТНАЯ РЕД РОЗЕБУД МОЛОДЫЕ РАСТЕНИЯ</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ФИКСАТОР-ШАР ХРОМ БОЛЬШОЙ, 1108349</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ВЕДРО ДЛЯ ЯГОД ПОЛИМЕРБЫТ 2 Л 4311001</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ТЮЛЬПАН LOUVRE (ЛУВР) БАХРОМЧАТЫЙ 25 ШТ</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ГЕРАНЬ ДОМАШНЯЯ (ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ) ЦВЕТУЩАЯ D12, H25-30, ЛОСОСЕВАЯ, МАХРОВАЯ</th>
      <td>16</td>
    </tr>
    <tr>
      <th>ЕМКОСТЬ ДЛЯ СОУСА С ЛОЖКОЙ PASABAHCE 200 МЛ OTS131505-000</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ТОМАТА (ПОМИДОР) ЮСУПОВСКИЙ УЗБЕКСКИЙ №86 СОРТ ИНДЕТЕРМИНАНТНЫЙ ПОЗДНЕСПЕЛЫЙ КРАСНЫЙ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ПРИМУЛА КОМНАТНАЯ D9 СМ ЦВЕТ В АССОРТИМЕНТЕ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ЭПИПРЕМНУМ АУРЕУМ D-12 СМ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ТЮЛЬПАН ПЛАСТИКОВЫЙ ОТКРЫТЫЙ ЯРКО-КРАСНЫЙ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>НАБОР ВЕШАЛОК МЯГКИХ ТКАНЕВЫХ С ПЕРЕКЛАДИНОЙ И КЛИПСАМИ 48Х23 СМ 2 ШТ ВАЛИАНТ / VALIANT VAL 7057GC</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ МУЛЬТИБЛУМ СКАРЛЕТ АЙ D-7 СМ H-10 СМ УКОРЕНЕННЫЙ ЧЕРЕНОК</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ЛИЛЕЙНИК ПЛАСТИКОВЫЙ СВЕТЛО-ЛИЛОВЫЙ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ РОЗА С ОТКРЫТИМ БУТОНОМ ЦВЕТ В АССОРТИМЕНТЕ 48 СМ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>РАССАДА ДЫНИ СОРТ ДИНА ГОРШОК 9Х9 СМ P-9</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ЦВЕТОК ИСКУССТВЕННЫЙ ТЮЛЬПАН 40 СМ ПЛАСТИК ЦВЕТ В АССОРТИМЕНТЕ</th>
      <td>15</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ РОЗОВАЯ С МАЛИНОВЫМ ПОЛУМАХРОВАЯ</th>
      <td>14</td>
    </tr>
    <tr>
      <th>ПЕТУНИЯ ПРОСТАЯ РАССАДА ОДНОЛЕТНИХ ЦВЕТОВ В КАССЕТЕ ПО 6 ШТ</th>
      <td>14</td>
    </tr>
    <tr>
      <th>ПЕТУНИЯ ПРОСТАЯ РАССАДА ОДНОЛЕТНИХ ЦВЕТОВ В КАССЕТЕ ПО 10 ШТ</th>
      <td>14</td>
    </tr>
    <tr>
      <th>ПОДАРОЧНЫЙ НАБОР НА 8 МАРТА ТЮЛЬПАНЫ РОЗОВЫЕ 5 ЛУКОВИЦ D-12 СМ В ПОДАРОЧНОЙ УПАКОВКЕ С ОТКРЫТКОЙ</th>
      <td>14</td>
    </tr>
    <tr>
      <th>ТИМЬЯН D-9 СМ</th>
      <td>14</td>
    </tr>
    <tr>
      <th>РАССАДА АРБУЗА СОРТ ПРОДЮСЕР, ГОРШОК 9*9СМ</th>
      <td>14</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ПЛАСТИКОВЫЙ МАК 3 БУТОНА 48 СМ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>МЫЛО РУЧНОЙ РАБОТЫ СУВЕНИР ПОДАРОК НА 8 МАРТА КЛАССИКА</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ЛАНДЫШ ПЛАСТИКОВЫЙ БОЛЬШОЙ БЕЛЫЙ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ТКАНЬ УНИВЕРСАЛЬНАЯ ИЗ МИКРОФИБРЫ MICRO MAGIC СЕРАЯ, LEIFHEIT, 40020</th>
      <td>13</td>
    </tr>
    <tr>
      <th>САЛФЕТКА НА СТОЛ 30X45 СМ ИЗ БАМБУКА NAPA-221</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ТОМАТА (ПОМИДОР) МОНГОЛЬСКИЙ КАРЛИК №53 СОРТ ДЕТЕРМИНАНТНЫЙ РАННЕСПЕЛЫЙ КРАСНЫЙ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>ТАЗ ПЛАСТМАССОВЫЙ 6,0 Л ПИЩЕВОЙ КВАДРАТНЫЙ "ТЮЛЬПАН" ПЦ2950, 1404003</th>
      <td>13</td>
    </tr>
    <tr>
      <th>РАССАДА ЗЕЛЕНИ ДЛЯ КУХНИ БАЗИЛИК ТОНУС, КАССЕТА ПО 6ШТ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>РАССАДА ДЫНИ СОРТ ЭФИОПКА ГОРШОК 9Х9 СМ P-9</th>
      <td>13</td>
    </tr>
    <tr>
      <th>РАССАДА ЗЕЛЕНИ ДЛЯ КУХНИ РОЗМАРИН БЛЮ ЛАГУН ДИАМ. 9 СМ</th>
      <td>13</td>
    </tr>
    <tr>
      <th>САЛФЕТКА МАХРОВАЯ РАДУГА В РАЗНОЦВЕТНУЮ ПОЛОСКУ, 100% ХЛОПОК 312 Г/М2, 31Х31 СМ, WELLNESS, 4630005360012</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ДЕСЕРТНАЯ БАМБУК 20 СМ J0276 LUMINARC</th>
      <td>12</td>
    </tr>
    <tr>
      <th>САЛАТНИК ВОЛАРЭ БЛЭК 16 СМ G9403 LUMINARC</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ОБЕДЕННАЯ ГРИН ОД 25 СМ G0707 LUMINARC</th>
      <td>12</td>
    </tr>
    <tr>
      <th>КОВРИК ХЛОПКОВЫЙ 40Х60 СМ ЦВЕТА В АССОРТИМЕНТЕ 6194</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА-ПЕРЕКЛАДИНА VALIANT МЕТАЛЛИЧЕСКАЯ ДВОЙНАЯ С АНТИСКОЛЬЗЯЩИМ ПОКРЫТИЕМ 50320</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ОДНОЛЕТНЕЕ РАСТЕНИЕ ЛОБЕЛИЯ В КАССЕТЕ ПО 4 ШТ, РОССИЯ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ ДИАМ. 12 СМ МАЛИНОВАЯ С КРАСНЫМ ПОЛУМАХРОВАЯ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК СИРЕНЬ ПЛАСТИКОВАЯ ЛИЛОВАЯ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>НАМАТРАЦНИК NATURES БАРХАТНЫЙ БАМБУК ББ-Н-1-2 ОДНОСПАЛЬНЫЙ, 100% ХЛОПОК, СТЕГАНЫЙ 90Х200 СМ, С РЕЗИНКОЙ ПО УГЛАМ, БЕЛЫЙ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ЗЕЛЕНОЕ ЯБЛОКО ПОЛИУРЕТАН D-6 СМ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>КОМПЛЕКТ МАХРОВЫХ САЛФЕТОК ТОП_2, 100% ХЛОПОК 266 Г/М2, 28Х42 СМ, 2 ШТ, ЦВЕТА В АССОРТИМЕНТЕ, WELLNESS, 4630005364393</th>
      <td>12</td>
    </tr>
    <tr>
      <th>САЛФЕТКА PROTEC TEXTIL LINO 30Х43 СМ ШОКОЛАД 6222</th>
      <td>12</td>
    </tr>
    <tr>
      <th>СУМКА-ТЕЛЕЖКА 2-Х КОЛЕСНАЯ СКЛАДНАЯ GIMI FLEXI ЗЕЛЕНАЯ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЕРЕВЯННАЯ С РАСШИРЕННЫМИ ПЛЕЧИКАМИ И ПЕРЕКЛАДИНОЙ 44,5Х23 СМ СМ БЕЛЫЙ JAPANESE WHITE VALIANT JW-19708</th>
      <td>12</td>
    </tr>
    <tr>
      <th>БАЗИЛИК ЗЕЛЕНЫЙ ТОНУС D-7 СМ</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ОБЕДЕННАЯ СТЕЛЛА ШОКОЛАД 25 СМ J1762 LUMINARC</th>
      <td>12</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ДЕСЕРТНАЯ LUMINARC СИНА СИЛЬВЕР 19 СМ L0886</th>
      <td>12</td>
    </tr>
    <tr>
      <th>РАССАДА ЗЕЛЕНИ ДЛЯ КУХНИ ТИМЬЯН ВАРИЕГЭЙТИД ДИАМ. 9 СМ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>КОВРИК ПРИДВЕРНЫЙ ХЛОПКОВЫЙ 60Х140 СМ МУЛЬТИКОЛОР, HELEX, С04</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК РОЗА МАРГАРИТА ПЛАСТИК 58 СМ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>РАССАДА ДЫНИ СОРТ КОЛХОЗНИЦА ГОРШОК 9Х9 СМ P-9</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ПЛАСТИКОВЫЙ НАРЦИСС 3 БУТОНА КОРОЛЕВСКИЙ 46 СМ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ВАКУУМНЫЙ ПАКЕТ EVA ДЛЯ ХРАНЕНИЯ ВЕЩЕЙ 50Х85 СМ EVEC0085</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ОДЕЯЛО WELLNESS T142 БЕЛОЕ ТЕМОСТЕГАНОЕ 140Х205 СМ ЧЕХОЛ 100% ПОЛИЭСТЕР 200 Г/М 4690659000306</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ГЕРАНЬ ДОМАШНЯЯ (ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ) ЦВЕТУЩАЯ , D12, H25-30, КРАСНАЯ, МАХРОВАЯ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>НАБОР ПРИЩЕПОК БОЛЬШИЕ ПЛАСТИКОВЫЕ ROZENBAL ПИНОККИО 10 ШТ R102312</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ТАРЕЛКА ДЕСЕРТНАЯ ВОЛАРЭ БЛЭК 22,5 СМ G9399 LUMINARC</th>
      <td>11</td>
    </tr>
    <tr>
      <th>РАССАДА ТОМАТА (ПОМИДОР) ЭФЕМЕР № 121 СОРТ ДЕТЕРМИНАНТНЫЙ РАННЕСПЕЛЫЙ КРАСНЫЙ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>РАССАДА ЗЕЛЕНИ ДЛЯ КУХНИ МЕЛИССА ЛИМОННАЯ ЗЕЛЕНЫЙ ОБЪЕМ 0,75 Л</th>
      <td>11</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ГРАНАТ 9 СМ КРАСНЫЙ</th>
      <td>11</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЛЯ БРЮК ATTRIBUTE CLASSIC 38 СМ AHN271</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ГЕРАНЬ ДОМАШНЯЯ (ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ) ЦВЕТУЩАЯ D12, H25-30, БЕЛАЯ, МАХРОВАЯ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СУШИЛКА ДЛЯ БЕЛЬЯ ПОТОЛОЧНАЯ ЛИАНА 2,0 М 1703009</th>
      <td>10</td>
    </tr>
    <tr>
      <th>РАССАДА ЗЕЛЕНИ ДЛЯ КУХНИ РОЗМАРИНА В ГОРШКЕ ДИАМ. 9 СМ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ВЕДРО AMPARI РТ9061СЛРОЗ-10РS 12 Л СЛИВОЧНО-РОЗОВЫЙ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СТОЛОВЫЙ НОЖ 2 ШТ САПФИР ТЯЖЕЛЫЙ СНSAPFIR-2</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СМЕТКА С СОВКОМ ПРЕСТИЖ, YORK, G6207</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ЩЕТКА ДЛЯ СМАХИВАНИЯ ПЫЛИ 60 СМ РАЗНОЦВЕТНАЯ, BURSTENMANN, 0528/0000</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК РОЗА БАРХАТНАЯ С ЗАКРЫТЫМ БУТОНОМ 72 СМ ЦВЕТ В АССОРТИМЕНТЕ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК РОЗА ЗАКРЫТАЯ МАЛАЯ ПЛАСТИКОВАЯ ЖЕЛТАЯ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЛЯ БРЮК МЕТАЛЛИЧЕСКАЯ С РЕЗИНОВЫМ ПОКРЫТИЕМ 26 СМ ЦВЕТ: СИНЯЯ, ATTRIBUTE, AHS331</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СКАТЕРТЬ КРУГЛАЯ D-175 СМ 50% ПОЛИЭСТЕР 50% ХЛОПОК БЕЛАЯ WELLNESS MLD-187-ЭСТЕЛЬ*01</th>
      <td>10</td>
    </tr>
    <tr>
      <th>СУШИЛКА ДЛЯ БЕЛЬЯ ПОТОЛОЧНАЯ ЛИАНА 1,7 М 1703006</th>
      <td>10</td>
    </tr>
    <tr>
      <th>РАССАДА ТОМАТА (ПОМИДОР) ШАЙМЕТ КРИГ № 117 СОРТ ДЕТЕРМИНАНТНЫЙ РАННЕСПЕЛЫЙ КРАСНЫЙ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ВЕШАЛКА ДЛЯ БРЮК ATTRIBUTE CLASSIC 30 СМ AHN261</th>
      <td>10</td>
    </tr>
    <tr>
      <th>МУЛЯЖ ПЕРЕЦ ЧИЛИ В СВЯЗКЕ, КРАСНЫЙ, 60 СМ, ПОЛИУРЕТАН, FANCY FAIR/FF RP60</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ТОМАТА (ПОМИДОР) МИНУСИНСКИЕ № 94 СОРТ ИНДЕТЕРМИНАНТНЫЙ СРЕДНЕСПЕЛЫЙ РОЗОВЫЙ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ИСКУССТВЕННЫЙ ЦВЕТОК ГВОЗДИКА САДОВАЯ ПЛАСТИКОВАЯ РОЗОВАЯ</th>
      <td>10</td>
    </tr>
    <tr>
      <th>КОМПЛЕКТ МАХРОВЫХ ПОЛОТЕНЕЦ 60Х30 СМ WELLNESS ПРОМО_3 100% ХЛОПОК</th>
      <td>10</td>
    </tr>
    <tr>
      <th>ПЕЛАРГОНИЯ ЗОНАЛЬНАЯ РОЗОЦВЕТНАЯ МАРГАРИТА ЛОСОСЕВАЯ 5-7 ЛИСТОВ</th>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>



С учетом представленных значений определим функцию для разбивке товаров на опредленные категории, внутри функции сформируем вспомогательный словарь.    

Разделим товары условно на следущие категории:   

  - ТОВАРЫ ДЛЯ ВАННОЙ
  - ЦВЕТЫ
  - ИСКУССТВЕННЫЕ ЦВЕТЫ
  - ТОВАРЫ ДЛЯ ДАЧИ
  - ТОВАРЫ ДЛЯ ДОМА
  - ПОСУДА
  - ХОЗТОВАРЫ
  - НАБОРЫ
  - РАЗНОЕ
  
  


```python

def create_category(_df_):
    
    dict={' ВАНН' : 'ТОВАРЫ ДЛЯ ВАННОЙ', 
          'СУШИЛКА' : 'ТОВАРЫ ДЛЯ ВАННОЙ', 
          'МЫЛ' : 'ТОВАРЫ ДЛЯ ВАННОЙ',
          'ВАНТУ' : 'ТОВАРЫ ДЛЯ ВАННОЙ',
          
          'ЦВЕТОК' : 'ЦВЕТЫ',
          'РОЗА' : 'ЦВЕТЫ',
          'ПЕЛАРГОНИЯ' : 'ЦВЕТЫ',
          'ПЕТУНИЯ' : 'ЦВЕТЫ',
          'ГЕРАНЬ' : 'ЦВЕТЫ',
          'ЦИПЕРУС' : 'ЦВЕТЫ',
          'ДЕКАБРИСТ' : 'ЦВЕТЫ',
          'БАЛЬЗАМИН' : 'ЦВЕТЫ',
          'АНТУРИУМ' : 'ЦВЕТЫ',
          'ПУАНСЕТТИЯ' : 'ЦВЕТЫ',
          'ЦИКЛАМЕН' : 'ЦВЕТЫ',
          'АФЕЛЯНДРА' : 'ЦВЕТЫ',
          'ХЛОРОФИТУМ' : 'ЦВЕТЫ',
          'НЕЗАБУДК' : 'ЦВЕТЫ',
          'ГОРШ' : 'ЦВЕТЫ',
          'РАСТЕН' : 'ЦВЕТЫ',
          'КАШПО' : 'ЦВЕТЫ',
          'D-5 СМ' : 'ЦВЕТЫ',
          'D-7 СМ' : 'ЦВЕТЫ',
          'D-8 СМ' : 'ЦВЕТЫ',
          'D-9 СМ' : 'ЦВЕТЫ',
          'D-10 СМ' : 'ЦВЕТЫ',
          'D-11 СМ' : 'ЦВЕТЫ',
          'D-12 СМ' : 'ЦВЕТЫ',
          'D-13 СМ' : 'ЦВЕТЫ',
          'D-15 СМ' : 'ЦВЕТЫ',
          'D-16 СМ' : 'ЦВЕТЫ',
          'D-17 СМ' : 'ЦВЕТЫ',
          'D-18 СМ' : 'ЦВЕТЫ',
          'D-19 СМ' : 'ЦВЕТЫ',               
          'D-20 СМ' : 'ЦВЕТЫ',         
          'D-21 СМ' : 'ЦВЕТЫ',                    
          'ОБЪЕМ ' : 'ЦВЕТЫ',
          'МИРТ' : 'ЦВЕТЫ',
          ' Г ' : 'ЦВЕТЫ',
          
          'ИСКУССТВЕННЫЙ ЦВЕТОК' : 'ИСКУССТВЕННЫЕ ЦВЕТЫ',
          
          'РАССАДА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ТОМАТ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ОДНОЛЕТНЕЕ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'БАКОПА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ПЕРЧАТКИ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'БАЗИЛИК' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'КАПУСТА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'КАЛИБРАХОА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'БЕГОНИЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ФУКСИЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ПРИМУЛА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'МЯТА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ФЛОКС' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ФИАЛКА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ТИМЬЯН' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ЛОБЕЛИЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ЭВКАЛИПТ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ДЫНЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ВИОЛА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ГАЗАНИЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ХРИЗАНТЕМА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'НАСТУРЦИЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ГВОЗДИКА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ВЕРБЕНА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ПЕТРУШКА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'КАЛАТЕЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'КОЛОКОЛЬЧИК' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'КОСМЕЯ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ПОДРУКАВНИК' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ЧЕРЕНОК' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'АРБУЗ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'КАССЕТ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ОГУРЕЦ' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ОСИНА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'ПАПОРОТНИК' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'УКРОП' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          'УРНА' : 'ТОВАРЫ ДЛЯ ДАЧИ',
          
                              
          'МУЛЯЖ' : 'ТОВАРЫ ДЛЯ ДОМА', 
          'ШТОР' : 'ТОВАРЫ ДЛЯ ДОМА', 
          'СКАТЕРТ' : 'ТОВАРЫ ДЛЯ ДОМА', 
          'ПОЛК' : 'ТОВАРЫ ДЛЯ ДОМА',
          'ДЕКОРАТИВ' : 'ТОВАРЫ ДЛЯ ДОМА',          
          'ДЕРЕВ' : 'ТОВАРЫ ДЛЯ ДОМА',
          'МАХР' : 'ТОВАРЫ ДЛЯ ДОМА',
          'ПЛЕД' : 'ТОВАРЫ ДЛЯ ДОМА',
          'ПОКРЫВ' : 'ТОВАРЫ ДЛЯ ДОМА',
          'ПОДУШ' : 'ТОВАРЫ ДЛЯ ДОМА',
          'ПОЛОТЕНЦ' : 'ТОВАРЫ ДЛЯ ДОМА',
          'ПРОСТЫН' : 'ТОВАРЫ ДЛЯ ДОМА',
          'НАМАТРА' : 'ТОВАРЫ ДЛЯ ДОМА',
          'НАВОЛОЧ' : 'ТОВАРЫ ДЛЯ ДОМА',
          
                    
          'ТАРЕЛК' : 'ПОСУДА',
          'САЛАТНИК' : 'ПОСУДА',
          'ЧАЙНИК' : 'ПОСУДА',
          'НОЖ' : 'ПОСУДА',
          'КАСТРЮЛЯ' : 'ПОСУДА',
          'ХЛЕБНИЦА' : 'ПОСУДА',
          'ТЕРМОКРУЖКА' : 'ПОСУДА',
          'СКОВОРОДА' : 'ПОСУДА',
          'КУВШИН' : 'ПОСУДА',
          'ВИЛКА' : 'ПОСУДА',
          'ЛОЖКА' : 'ПОСУДА',
          'КРУЖКА' : 'ПОСУДА',
          'БЛЮД' : 'ПОСУДА',
          'МИСК' : 'ПОСУДА',
          'СТАКАН' : 'ПОСУДА',
          'ТЕРМОС' : 'ПОСУДА',
                    
                    
          'СУМКА' : 'ХОЗТОВАРЫ',            
          'ТЕЛЕЖК' : 'ХОЗТОВАРЫ',            
          'ГЛАДИЛЬН' : 'ХОЗТОВАРЫ',
          'КОВРИК' : 'ХОЗТОВАРЫ',
          'ЧЕХОЛ' : 'ХОЗТОВАРЫ',
          'ТЕЛЕЖК' : 'ХОЗТОВАРЫ',          
          'ТАЗ' : 'ХОЗТОВАРЫ',
          'КОРЗИН' : 'ХОЗТОВАРЫ',
          'КОНТЕЙНЕР' : 'ХОЗТОВАРЫ',
          'ЛЕСТНИЦ' : 'ХОЗТОВАРЫ',
          'СТРЕМЯНК' : 'ХОЗТОВАРЫ',
          'ШВАБРА' : 'ХОЗТОВАРЫ',
          'ЩЕТК' : 'ХОЗТОВАРЫ',
          'ВЕШАЛК' : 'ХОЗТОВАРЫ',
          'ПЛЕЧИКИ' : 'ХОЗТОВАРЫ',
          'КАРНИЗ' : 'ХОЗТОВАРЫ',
          'ВЕДР' : 'ХОЗТОВАРЫ',
          'ВЕСЫ' : 'ХОЗТОВАРЫ',
          'ЯЩИК' : 'ХОЗТОВАРЫ',
          'КРЮЧОК' : 'ХОЗТОВАРЫ',
          'БАНКА' : 'ХОЗТОВАРЫ',
          'ЁРШ' : 'ХОЗТОВАРЫ',
          'САЛФЕТ' : 'ХОЗТОВАРЫ',
          'ПОДСТАВК' : 'ХОЗТОВАРЫ',
          'ОКНОМОЙКА' : 'ХОЗТОВАРЫ',
          'СИДЕНЬЕ' : 'ХОЗТОВАРЫ',
          'КОВЕР' : 'ХОЗТОВАРЫ',
          'КОФР' : 'ХОЗТОВАРЫ',
          'ШТАНГА' : 'ХОЗТОВАРЫ',
          'ПОДКЛАДКА' : 'ХОЗТОВАРЫ',
          'РУКАВ' : 'ХОЗТОВАРЫ',
          'СРЕДСТВ' : 'ХОЗТОВАРЫ',
          'КОРЫТ' : 'ХОЗТОВАРЫ',
          'ОВОЩ' : 'ХОЗТОВАРЫ',
          'КРЫШК' : 'ХОЗТОВАРЫ',
          'ТЕРМОМ' : 'ХОЗТОВАРЫ',
          'МЕШО' : 'ХОЗТОВАРЫ',
          'ПЕТЛ' : 'ХОЗТОВАРЫ',
          'СТЯЖК' : 'ХОЗТОВАРЫ',
          'БЕНЗ' : 'ХОЗТОВАРЫ',
          'ТРЯПК' : 'ХОЗТОВАРЫ',
          'ВАРК' : 'ХОЗТОВАРЫ',
          'МИКС' : 'ХОЗТОВАРЫ',
          'ЗУБН' : 'ХОЗТОВАРЫ',
          'БИДОН' : 'ХОЗТОВАРЫ',
          'ВЕРЕВК' : 'ХОЗТОВАРЫ',                 
          'ДОСК' : 'ХОЗТОВАРЫ',                 
          'КОВШ' : 'ХОЗТОВАРЫ',                 
          'КОРОБК' : 'ХОЗТОВАРЫ',                 
          'ШТАНГЕН' : 'ХОЗТОВАРЫ',                 
          'СОВОК' : 'ХОЗТОВАРЫ',                 
          'ТЕРК' : 'ХОЗТОВАРЫ',       
          'СТЕЛЛ' : 'ХОЗТОВАРЫ',                 
          'ПРОСЕИВАТ' : 'ХОЗТОВАРЫ',                 
          'КАПРОН' : 'ХОЗТОВАРЫ',                 
          'КОМОД' : 'ХОЗТОВАРЫ',                 
          'БАК ' : 'ХОЗТОВАРЫ',                 
          'КИСТОЧ' : 'ХОЗТОВАРЫ',                 
          
          
          
          
          'НАБОР' : 'НАБОРЫ',
          'КОМПЛЕКТ' : 'НАБОРЫ',
          'ПОДАРОЧНЫ' : 'НАБОРЫ',
                   

         }
          
    _df_['category'] = 'РАЗНОЕ'
    
    for key, category in dict.items():
                    
       
        _df_.loc[_df_['product'].str.contains(key, regex=True), 'category'] = category       
                     
    return _df_
```

Формируем категории товаров:


```python

df = create_category(df)
df.info()

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 5001 entries, 0 to 7473
    Data columns (total 17 columns):
     #   Column         Non-Null Count  Dtype         
    ---  ------         --------------  -----         
     0   date           5001 non-null   datetime64[ns]
     1   customer_id    5001 non-null   object        
     2   order_id       5001 non-null   int64         
     3   product        5001 non-null   object        
     4   quantity       5001 non-null   int64         
     5   price          5001 non-null   float64       
     6   year           5001 non-null   int64         
     7   week           5001 non-null   int64         
     8   month          5001 non-null   int64         
     9   month_name     5001 non-null   object        
     10  day            5001 non-null   int64         
     11  week_day       5001 non-null   int64         
     12  week_day_name  5001 non-null   object        
     13  hour           5001 non-null   int64         
     14  total_price    5001 non-null   float64       
     15  product_sstr   5001 non-null   object        
     16  category       5001 non-null   object        
    dtypes: datetime64[ns](1), float64(2), int64(8), object(6)
    memory usage: 703.3+ KB
    

В итоге получили следующую разбивку товаров по категориям:


```python

df['category'].unique() 

```




    array(['ЦВЕТЫ', 'ТОВАРЫ ДЛЯ ВАННОЙ', 'ХОЗТОВАРЫ', 'ТОВАРЫ ДЛЯ ДАЧИ',
           'ТОВАРЫ ДЛЯ ДОМА', 'ПОСУДА', 'НАБОРЫ', 'РАЗНОЕ',
           'ИСКУССТВЕННЫЕ ЦВЕТЫ'], dtype=object)



### Исследовательский анализ данных

Определим временной период, за который представлены данные в датасете.


```python

print('Данные представлены за период:')
print('с ', df['date'].min())
print('по ', df['date'].max())

```

    Данные представлены за период:
    с  2018-10-01 00:00:00
    по  2020-01-31 15:00:00
    

Построим гистограммы полей и проведем предварительный анализ:



```python

df.hist(figsize=(15, 20), bins=50)
None

```


    
![png](output_104_0.png)
    


Предварительные выводы по гистограммам (по частоте и распределению событий):

 - наблюдается неравномерное распределение по количеству заказов в течение рассматриваемого периода, есть "всплески" и провалы; 
 - вероятно, что разброс по нумерации заказов, обусловлен наличием различных категорий товаров;
 - подавляющее большинство клиентов приобретают малые партии товаров;
 - основная часть покупок - это недорогие товары;
 - наблюдается сезонность в продажах, с провалами в феврале и марте, а также в период летних отпусков (с июня по сентябрь)
 - по дням недели: больше всего продаж - в понедельник, меньше всего  - в субботу;
 - по часам: наблюдается спад продаж в ночные часы (примерно с 2-х часов до 6).
 - основная часть заказов в датасете - это заказы за 2019 г.

#### Распределение товаров по категориям


```python

df_cat = df.pivot_table(index=['category'], values='product', \
                        aggfunc='nunique').sort_values('product', ascending=False).reset_index()

df_cat.columns=['category','product_counter']
df_cat

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>product_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ХОЗТОВАРЫ</td>
      <td>702</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ТОВАРЫ ДЛЯ ДАЧИ</td>
      <td>467</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ЦВЕТЫ</td>
      <td>309</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ТОВАРЫ ДЛЯ ДОМА</td>
      <td>258</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ПОСУДА</td>
      <td>145</td>
    </tr>
    <tr>
      <th>5</th>
      <td>ТОВАРЫ ДЛЯ ВАННОЙ</td>
      <td>122</td>
    </tr>
    <tr>
      <th>6</th>
      <td>РАЗНОЕ</td>
      <td>110</td>
    </tr>
    <tr>
      <th>7</th>
      <td>НАБОРЫ</td>
      <td>50</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ИСКУССТВЕННЫЕ ЦВЕТЫ</td>
      <td>33</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_cat = df.pivot_table(index=['category'], \
                        values='product', aggfunc='nunique').sort_values('product', ascending=False).\
plot(kind='bar', figsize=(10,3), color='lightblue', legend=False, use_index=True)

plt.title('Распределение товаров по категориям', fontsize=12)
plt.xlabel("категория", fontsize=10)
plt.ylabel("количество товаров", fontsize=10)

plt.xticks(rotation=45)
plt.tick_params(length=4)

plt.show()
None

```


    
![png](output_108_0.png)
    


Больше всего товаров представлено в категории `ХОЗТОВАРЫ`.

#### Распределение заказов по категориям


```python

df_cat = df.pivot_table(index=['category'], values='order_id', \
                        aggfunc='nunique').sort_values('order_id', ascending=False).reset_index()
df_cat.columns=['category','orders_counter']
df_cat

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ХОЗТОВАРЫ</td>
      <td>1329</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ТОВАРЫ ДЛЯ ДАЧИ</td>
      <td>536</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ТОВАРЫ ДЛЯ ДОМА</td>
      <td>508</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ЦВЕТЫ</td>
      <td>418</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ТОВАРЫ ДЛЯ ВАННОЙ</td>
      <td>299</td>
    </tr>
    <tr>
      <th>5</th>
      <td>ПОСУДА</td>
      <td>183</td>
    </tr>
    <tr>
      <th>6</th>
      <td>РАЗНОЕ</td>
      <td>149</td>
    </tr>
    <tr>
      <th>7</th>
      <td>НАБОРЫ</td>
      <td>63</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ИСКУССТВЕННЫЕ ЦВЕТЫ</td>
      <td>55</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_cat = df.pivot_table(index=['category'], \
                        values='order_id', aggfunc='nunique').sort_values('order_id', ascending=False).\
plot(kind='bar', figsize=(10,3), color='lightblue', legend=False, use_index=True)

plt.title('Распределение заказов по категориям', fontsize=12)
plt.xlabel("категория", fontsize=10)
plt.ylabel("количество заказов", fontsize=10)

plt.xticks(rotation=45)
plt.tick_params(length=4)

plt.show()
None

```


    
![png](output_112_0.png)
    


По количеству заказов также лидирует категория `ХОЗТОВАРЫ`.

#### Динамика привлечения новых пользователей по месяцам


```python

df['customer_id'].describe()

```




    count                                     5001
    unique                                    2185
    top       C971FB21-D54C-4134-938F-16B62EE86D3B
    freq                                       142
    Name: customer_id, dtype: object




```python

dfc = df.pivot_table(
    index=['year','month'],  
    values='customer_id',
    aggfunc='nunique', 
    margins=True
)
dfc

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>customer_id</th>
    </tr>
    <tr>
      <th>year</th>
      <th>month</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="3" valign="top">2018</th>
      <th>10</th>
      <td>169</td>
    </tr>
    <tr>
      <th>11</th>
      <td>166</td>
    </tr>
    <tr>
      <th>12</th>
      <td>202</td>
    </tr>
    <tr>
      <th rowspan="12" valign="top">2019</th>
      <th>1</th>
      <td>134</td>
    </tr>
    <tr>
      <th>2</th>
      <td>219</td>
    </tr>
    <tr>
      <th>3</th>
      <td>200</td>
    </tr>
    <tr>
      <th>4</th>
      <td>214</td>
    </tr>
    <tr>
      <th>5</th>
      <td>147</td>
    </tr>
    <tr>
      <th>6</th>
      <td>132</td>
    </tr>
    <tr>
      <th>7</th>
      <td>164</td>
    </tr>
    <tr>
      <th>8</th>
      <td>153</td>
    </tr>
    <tr>
      <th>9</th>
      <td>156</td>
    </tr>
    <tr>
      <th>10</th>
      <td>152</td>
    </tr>
    <tr>
      <th>11</th>
      <td>217</td>
    </tr>
    <tr>
      <th>12</th>
      <td>246</td>
    </tr>
    <tr>
      <th>2020</th>
      <th>1</th>
      <td>269</td>
    </tr>
    <tr>
      <th>All</th>
      <th></th>
      <td>2185</td>
    </tr>
  </tbody>
</table>
</div>




```python

df.pivot_table(index=['year','month'], values='customer_id', \
               aggfunc='nunique').plot(figsize=(10, 7), title='Динамика привлечения новых пользователей')

plt.show()

```


    
![png](output_117_0.png)
    


Видно провал в период отпусков в летние месяцы и рост в конце года.

#### Динамика привлечения новых пользователей по категориям по месяцам


```python

df.pivot_table(index=['year','month'], columns='category', values='customer_id', \
               aggfunc='nunique').plot(figsize=(10, 7), title='Динамика привлечения новых пользователей по категориям')

plt.show()

```


    
![png](output_120_0.png)
    


На графике видно какой вклад вносит каждая категория товара. В лидерах `ХОЗТОВАРЫ` и `ТОВАРЫ ДЛЯ ДАЧИ`.


#### Распределение заказов по покупателям
    


```python

df_order_by_customer= df.groupby('customer_id').agg({'order_id' : 'nunique'}\
                                                   ).sort_values(by='order_id', ascending=False).reset_index()
df_order_by_customer.columns = ['customer_id', 'orders_counter']
df_order_by_customer.info()
df_order_by_customer.head(10)

```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 2185 entries, 0 to 2184
    Data columns (total 2 columns):
     #   Column          Non-Null Count  Dtype 
    ---  ------          --------------  ----- 
     0   customer_id     2185 non-null   object
     1   orders_counter  2185 non-null   int64 
    dtypes: int64(1), object(1)
    memory usage: 34.3+ KB
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>C971FB21-D54C-4134-938F-16B62EE86D3B</td>
      <td>125</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</td>
      <td>34</td>
    </tr>
    <tr>
      <th>2</th>
      <td>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</td>
      <td>16</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B7B865AB-0735-407F-8D0C-31F74D2806CC</td>
      <td>7</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E0535076-6270-4DF2-8621-CB06264A94FA</td>
      <td>4</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BEA7A833-2074-42DB-BC49-4457ABD3C930</td>
      <td>4</td>
    </tr>
    <tr>
      <th>6</th>
      <td>498F12A4-6A62-4725-8516-CF5DC9AB8A3A</td>
      <td>4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>4856A2A7-B9D2-4243-B8D9-A96EC1425BBE</td>
      <td>3</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6B0C6CFB-7717-4C34-8535-BBC6E2B2C758</td>
      <td>3</td>
    </tr>
    <tr>
      <th>9</th>
      <td>EB6521AE-56E3-4A72-9EA2-E9C69701FF3F</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_order_by_customer.describe()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>2185.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>1.481007</td>
    </tr>
    <tr>
      <th>std</th>
      <td>2.802199</td>
    </tr>
    <tr>
      <th>min</th>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>2.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>125.000000</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_order_by_customer.plot(kind='hist', range=[0, 130],
                          bins=30,
                          title='Количество заказов на покупателя', color='lightblue',
          figsize=(10,3)
         )
None

```


    
![png](output_125_0.png)
    



```python

df_order_by_customer.plot(kind='hist', range=[0, 10],
                          bins=30,
                          title='Количество заказов на покупателя', color='lightblue',
          figsize=(10,3)
         )
None

```


    
![png](output_126_0.png)
    



Максимальное количество заказов - 125, минимальное - 1.   
Больше половины всех пользователей оформили не более 1 заказа, еще четверть - не более 2-х заказов.  



#### Распределение заказов по месяцам


```python

df_order_by_month= df.groupby(['year', 'month']).agg({'order_id' : 'nunique'}).sort_values(by=['year', 'month']).reset_index()
df_order_by_month.columns = ['year', 'month', 'orders_counter']
df_order_by_month.info()
df_order_by_month.head(10)

```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 16 entries, 0 to 15
    Data columns (total 3 columns):
     #   Column          Non-Null Count  Dtype
    ---  ------          --------------  -----
     0   year            16 non-null     int64
     1   month           16 non-null     int64
     2   orders_counter  16 non-null     int64
    dtypes: int64(3)
    memory usage: 516.0 bytes
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>month</th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018</td>
      <td>10</td>
      <td>226</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018</td>
      <td>11</td>
      <td>192</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018</td>
      <td>12</td>
      <td>251</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2019</td>
      <td>1</td>
      <td>149</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2019</td>
      <td>2</td>
      <td>259</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2019</td>
      <td>3</td>
      <td>213</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2019</td>
      <td>4</td>
      <td>238</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2019</td>
      <td>5</td>
      <td>161</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2019</td>
      <td>6</td>
      <td>140</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2019</td>
      <td>7</td>
      <td>175</td>
    </tr>
  </tbody>
</table>
</div>




```python

df.pivot_table(index=['year','month'], values='order_id', aggfunc='nunique')\
.sort_values(['year', 'month']).plot(kind='bar', figsize=(10,3), color='lightblue', 
                                          legend=False, use_index=True)

plt.title('Распределение заказов по месяцам', fontsize=12)
plt.xlabel("год, месяц", fontsize=10)
plt.ylabel("количество заказов", fontsize=10)

plt.xticks(rotation=45)

plt.show()
None

```


    
![png](output_130_0.png)
    



```python

df.pivot_table(index=['year','month'], values='order_id', aggfunc='nunique').sort_values(['year', 'month'])

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>order_id</th>
    </tr>
    <tr>
      <th>year</th>
      <th>month</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="3" valign="top">2018</th>
      <th>10</th>
      <td>226</td>
    </tr>
    <tr>
      <th>11</th>
      <td>192</td>
    </tr>
    <tr>
      <th>12</th>
      <td>251</td>
    </tr>
    <tr>
      <th rowspan="12" valign="top">2019</th>
      <th>1</th>
      <td>149</td>
    </tr>
    <tr>
      <th>2</th>
      <td>259</td>
    </tr>
    <tr>
      <th>3</th>
      <td>213</td>
    </tr>
    <tr>
      <th>4</th>
      <td>238</td>
    </tr>
    <tr>
      <th>5</th>
      <td>161</td>
    </tr>
    <tr>
      <th>6</th>
      <td>140</td>
    </tr>
    <tr>
      <th>7</th>
      <td>175</td>
    </tr>
    <tr>
      <th>8</th>
      <td>161</td>
    </tr>
    <tr>
      <th>9</th>
      <td>162</td>
    </tr>
    <tr>
      <th>10</th>
      <td>177</td>
    </tr>
    <tr>
      <th>11</th>
      <td>217</td>
    </tr>
    <tr>
      <th>12</th>
      <td>246</td>
    </tr>
    <tr>
      <th>2020</th>
      <th>1</th>
      <td>269</td>
    </tr>
  </tbody>
</table>
</div>




```python

plot = sns.catplot(x='month_name', y='order_id', kind="bar", hue="year",alpha=.8, palette="muted", legend=False, \
    data=df.pivot_table(index=['year','month', 'month_name'], values='order_id', \
                        aggfunc='nunique').sort_values(['year', 'month']).reset_index())

plot.set_titles(row_template='2', col_template='1')

plot.fig.set_size_inches(10,3)
plt.title("Распределение заказов по месяцам", fontsize=12)
plt.xlabel('Месяц', fontsize=10)
plt.ylabel('Количество заказов', fontsize=10)
plt.xticks(rotation=45)
plt.tick_params(length=4)
plt.legend()
plt.show()

```


    
![png](output_132_0.png)
    


Анализ показывает, что в течение 2019 года наблюдается определенная "сезонность", например: рост продаж в феврале, марте, апреле, декабре...что, вероятнее всего, обусловлено покупками подарков к праздникам (23 февраля, 8 марта, Новый год) и подготовкой к дачному сезону (апрель).   
Если сравнить данные год к году (2018/2019) за октябрь, ноябрь и декабрь, то видно, что продажи в 2019 г. уступают 2018 г. в октябре и декабре, но превышают в ноябре.  
Существенный рост в январе 2020 г. относительно января 2019 и декабря 2019 может быть обусловлен, например, проведением  в декабре 2019 г. рекламной акции по продвижению продарочных новогодних сертификатов.

#### Распределение заказов по дням недели


```python

plot = sns.catplot(x='week_day_name', y='order_id', kind="bar", hue="year",alpha=.8, palette="muted", legend=False, \
    data=df.pivot_table(index=['year','week_day', 'week_day_name'], values='order_id', \
                        aggfunc='nunique').sort_values(['year', 'week_day']).reset_index())

plot.set_titles(row_template='2', col_template='1')

plot.fig.set_size_inches(10,3)
plt.title("Распределение заказов по дням недели", fontsize=12)
plt.xlabel('день недели', fontsize=10)
plt.ylabel('количество заказов', fontsize=10)
plt.xticks(rotation=45)
plt.tick_params(length=4)
plt.legend()
plt.show()

```


    
![png](output_135_0.png)
    


Анализ по дням недели показывает спад продаж по субботам. Наиболее активные продажи в общем за период наблюдаются в понедельник и вторник. При этом внутри месяца (на примере данных за январь 2020 г) активность по дням недели может различаться от месяца к месяцу.

#### Динамика выручки по месяцам и неделям


```python

plt.figure(figsize=(12, 10))

#------------------------------------------------

ax1 = plt.subplot(2, 1, 1)
df.pivot_table(index='month', columns='year', values = 'total_price', aggfunc = 'sum').plot(
    grid=True, ax=ax1, figsize=(10, 10)
)

plt.legend()
plt.xlabel('месяц')
plt.title('Динамика выручки по месяцам')

#------------------------------------------------

ax2 = plt.subplot(2, 1, 2)

df.pivot_table(index='week', columns='year', values = 'total_price', aggfunc = 'sum').plot(
    grid=True, ax=ax2, figsize=(10, 10)
)
 
plt.legend()
plt.xlabel('неделя')
plt.title('Динамика выручки по неделям')
#------------------------------------------------

None

```


    
![png](output_138_0.png)
    


Мы видим всплеск в июне 2019 г, когда были закуплены крупные оптовые партии отдельных товаров. А также заметен спад, начиная с июля 2019 и до конца 2019 года.

#### Динамика среднего чека по месяцам


```python

df.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
      <th>year</th>
      <th>week</th>
      <th>month</th>
      <th>month_name</th>
      <th>day</th>
      <th>week_day</th>
      <th>week_day_name</th>
      <th>hour</th>
      <th>total_price</th>
      <th>product_sstr</th>
      <th>category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ АЛОЕ ВЕРА, D12, H30</td>
      <td>1</td>
      <td>142.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>142.0</td>
      <td>КОМНАТНОЕ</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ КОФЕ АРАБИКА, D12, H25</td>
      <td>1</td>
      <td>194.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>194.0</td>
      <td>КОМНАТНОЕ</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>РАДЕРМАХЕРА D-12 СМ H-20 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
      <td>РАДЕРМАХЕРА</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ХРИЗОЛИДОКАРПУС ЛУТЕСЦЕНС D-9 СМ</td>
      <td>1</td>
      <td>179.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>179.0</td>
      <td>ХРИЗОЛИДОКАРПУС</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ЦИПЕРУС ЗУМУЛА D-12 СМ H-25 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
      <td>ЦИПЕРУС</td>
      <td>ЦВЕТЫ</td>
    </tr>
  </tbody>
</table>
</div>




```python

cheque = df.groupby(df['date'].dt.strftime('%Y.%m')).agg({'total_price': 'sum','order_id': 'nunique'}\
                                                        ).reset_index(inplace = False)
cheque.columns = ['date', 'total_price', 'orders_counter']
cheque['cheque_avg'] = cheque['total_price']/cheque['orders_counter']
cheque.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>total_price</th>
      <th>orders_counter</th>
      <th>cheque_avg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018.10</td>
      <td>302390.0</td>
      <td>226</td>
      <td>1338.008850</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018.11</td>
      <td>351120.0</td>
      <td>192</td>
      <td>1828.750000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018.12</td>
      <td>320184.0</td>
      <td>251</td>
      <td>1275.633466</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2019.01</td>
      <td>209208.0</td>
      <td>149</td>
      <td>1404.080537</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2019.02</td>
      <td>270730.0</td>
      <td>259</td>
      <td>1045.289575</td>
    </tr>
  </tbody>
</table>
</div>




```python

cheque.plot(x='date', y='cheque_avg', figsize=(10, 3), color='green')

plt.xlabel('месяц')
plt.ylabel('сумма')
plt.title('Динамика среднего чека по месяцам', fontsize = 12)

plt.show();

```


    
![png](output_143_0.png)
    


Анализ за весь период показывает отрицательную динамику за исключением всплеска в июне 2019 г. (оптовые покупки).
Рассмотрим далее динамику среднего чека по категориям товаров.


```python

cheque = df.groupby([df['date'].dt.strftime('%Y.%m'), 'category']).agg({'total_price': 'sum','order_id': 'nunique'}\
                                                        ).reset_index(inplace = False).sort_values(['date','category'])
cheque.columns = ['date', 'category', 'total_price', 'orders_counter']
cheque['cheque_avg'] = cheque['total_price']/cheque['orders_counter']
cheque.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>category</th>
      <th>total_price</th>
      <th>orders_counter</th>
      <th>cheque_avg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018.10</td>
      <td>ИСКУССТВЕННЫЕ ЦВЕТЫ</td>
      <td>1144.0</td>
      <td>3</td>
      <td>381.333333</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018.10</td>
      <td>НАБОРЫ</td>
      <td>11788.0</td>
      <td>7</td>
      <td>1684.000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018.10</td>
      <td>ПОСУДА</td>
      <td>12765.0</td>
      <td>10</td>
      <td>1276.500000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018.10</td>
      <td>РАЗНОЕ</td>
      <td>6365.0</td>
      <td>9</td>
      <td>707.222222</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018.10</td>
      <td>ТОВАРЫ ДЛЯ ВАННОЙ</td>
      <td>34709.0</td>
      <td>38</td>
      <td>913.394737</td>
    </tr>
  </tbody>
</table>
</div>




```python

plt.figure(figsize=(15, 10))

#------------------------------------------------

ax1 = plt.subplot(2, 1, 1)
cheque.pivot_table(index=['date'], columns='category', values = 'cheque_avg', aggfunc = 'sum').plot(
    grid=True, ax=ax1, figsize=(10, 15)
)

plt.legend()
plt.xlabel('дата')
plt.title('Динамика среднего чека по категориям')

#------------------------------------------------

ax2 = plt.subplot(2, 1, 2)

cheque.pivot_table(index=['date'], columns='category', \
                   values = 'cheque_avg', aggfunc = 'sum').drop(columns=['ТОВАРЫ ДЛЯ ДОМА']).plot(
    grid=True, ax=ax2, figsize=(10, 15)
)
 
plt.legend()
plt.xlabel('дата')
plt.title('Динамика среднего чека по категориям (без категории "ТОВАРЫ ДЛЯ ДОМА")')
#------------------------------------------------

None

```


    
![png](output_146_0.png)
    



Анализ динамики среднего чека по категориям товаров показывает наиболее существенные "провалы" в категории "НАБОРЫ" и "качели" в остальных категориях.

### Сегментация пользователей

#### Сегменты по количеству оформленных заказов


```python

df.info()
df.head()

```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 5001 entries, 0 to 7473
    Data columns (total 17 columns):
     #   Column         Non-Null Count  Dtype         
    ---  ------         --------------  -----         
     0   date           5001 non-null   datetime64[ns]
     1   customer_id    5001 non-null   object        
     2   order_id       5001 non-null   int64         
     3   product        5001 non-null   object        
     4   quantity       5001 non-null   int64         
     5   price          5001 non-null   float64       
     6   year           5001 non-null   int64         
     7   week           5001 non-null   int64         
     8   month          5001 non-null   int64         
     9   month_name     5001 non-null   object        
     10  day            5001 non-null   int64         
     11  week_day       5001 non-null   int64         
     12  week_day_name  5001 non-null   object        
     13  hour           5001 non-null   int64         
     14  total_price    5001 non-null   float64       
     15  product_sstr   5001 non-null   object        
     16  category       5001 non-null   object        
    dtypes: datetime64[ns](1), float64(2), int64(8), object(6)
    memory usage: 703.3+ KB
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
      <th>year</th>
      <th>week</th>
      <th>month</th>
      <th>month_name</th>
      <th>day</th>
      <th>week_day</th>
      <th>week_day_name</th>
      <th>hour</th>
      <th>total_price</th>
      <th>product_sstr</th>
      <th>category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ АЛОЕ ВЕРА, D12, H30</td>
      <td>1</td>
      <td>142.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>142.0</td>
      <td>КОМНАТНОЕ</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ КОФЕ АРАБИКА, D12, H25</td>
      <td>1</td>
      <td>194.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>194.0</td>
      <td>КОМНАТНОЕ</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>РАДЕРМАХЕРА D-12 СМ H-20 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
      <td>РАДЕРМАХЕРА</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ХРИЗОЛИДОКАРПУС ЛУТЕСЦЕНС D-9 СМ</td>
      <td>1</td>
      <td>179.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>179.0</td>
      <td>ХРИЗОЛИДОКАРПУС</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ЦИПЕРУС ЗУМУЛА D-12 СМ H-25 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
      <td>ЦИПЕРУС</td>
      <td>ЦВЕТЫ</td>
    </tr>
  </tbody>
</table>
</div>



Определим количество уникальных пользователей в датасете.


```python

print()
print('Количество уникальных пользователей в датасете: ', len(df['customer_id'].unique()))
print()

```

    
    Количество уникальных пользователей в датасете:  2185
    
    

Разделим всех пользователей на сегменты в зависимости от количества сделанных заказов.
Построим сводную таблицу для оценки...


```python

df_seg_orders_num = df.pivot_table(index='customer_id', values='order_id',\
                                   aggfunc='nunique').sort_values('order_id', ascending=False).reset_index()
df_seg_orders_num.columns = ['customer_id', 'orders_counter']
df_seg_orders_num.head(10)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>C971FB21-D54C-4134-938F-16B62EE86D3B</td>
      <td>125</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</td>
      <td>34</td>
    </tr>
    <tr>
      <th>2</th>
      <td>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</td>
      <td>16</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B7B865AB-0735-407F-8D0C-31F74D2806CC</td>
      <td>7</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E0535076-6270-4DF2-8621-CB06264A94FA</td>
      <td>4</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BEA7A833-2074-42DB-BC49-4457ABD3C930</td>
      <td>4</td>
    </tr>
    <tr>
      <th>6</th>
      <td>498F12A4-6A62-4725-8516-CF5DC9AB8A3A</td>
      <td>4</td>
    </tr>
    <tr>
      <th>7</th>
      <td>4856A2A7-B9D2-4243-B8D9-A96EC1425BBE</td>
      <td>3</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6B0C6CFB-7717-4C34-8535-BBC6E2B2C758</td>
      <td>3</td>
    </tr>
    <tr>
      <th>9</th>
      <td>EB6521AE-56E3-4A72-9EA2-E9C69701FF3F</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_seg_orders_num.tail(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2180</th>
      <td>6E34F9F8-2E96-48D3-A282-66C71A43BF48</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2181</th>
      <td>6DDD4081-515E-401A-9567-8C0EAB6DC868</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2182</th>
      <td>6DBAE21E-4B94-4B16-9548-24AADF146AE2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2183</th>
      <td>6DA7E5E8-060B-4116-91EF-015B33965D68</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2184</th>
      <td>FFE82299-3F5B-4214-87FE-3D36ECCCFAC3</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python

print()
print('Всего уникальных значений - вариантов по количеству заказов: ', len(df_seg_orders_num['orders_counter'].unique()))
print()

```

    
    Всего уникальных значений - вариантов по количеству заказов:  8
    
    


Распределение количества пользователей по количеству заказов:



```python

df_seg_orders_num_usr = df_seg_orders_num.groupby(['orders_counter']).agg(
    {'customer_id' : 'count'}).sort_values('customer_id', ascending=False).reset_index()

df_seg_orders_num_usr.columns = ['orders_counter', 'customers_counter']
df_seg_orders_num_usr

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>orders_counter</th>
      <th>customers_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1333</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>826</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>19</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>7</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>16</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>34</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>125</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_seg_orders_num_usr.hist(bins=30)
None

```


    
![png](output_159_0.png)
    



```python

df_seg_orders_num_usr['customers_counter'].describe()

```




    count       8.000000
    mean      273.125000
    std       515.857244
    min         1.000000
    25%         1.000000
    50%         2.000000
    75%       220.750000
    max      1333.000000
    Name: customers_counter, dtype: float64




```python

df_seg_orders_num_usr.plot(x='orders_counter', y='customers_counter', figsize=(10, 3))

plt.xlabel('количество заказов')
plt.ylabel('количество клиентов')
plt.title('Соотношение количества заказов и клиентов', fontsize = 12)

plt.show();

```


    
![png](output_161_0.png)
    



```python

df_seg_orders_num_usr.query('orders_counter < 10').plot(x='orders_counter', \
                                                        y='customers_counter', figsize=(10, 3))

plt.xlabel('количество заказов')
plt.ylabel('количество клиентов')
plt.title('Соотношение количества заказов и клиентов', fontsize = 12)

plt.show();

```


    
![png](output_162_0.png)
    


Анализ показывает, что большая часть пользователей оформили 1 или 2 заказа. Поэтому по количеству сделанных заказов целесообразно разделить всех клиентов на 3 категории:  
 - 'один заказ'
 - 'два заказа'
 - 'более двух заказов'.

Определим функцию для формирования категорий при сегментировании клиентов по количеству сделанных заказов:


```python

def create_segment_1_category(_df_):                   
    
    _df_.loc[_df_['orders_counter'] == 1, 'seg_1_category'] = 'один заказ'
    
    _df_.loc[_df_['orders_counter'] == 2, 'seg_1_category'] = 'два заказа'
    
    _df_.loc[_df_['orders_counter'] > 2, 'seg_1_category'] = 'более двух заказов'
    
                     
    return _df_
```


```python

df_usr_segments_1 = create_segment_1_category(df_seg_orders_num)
df_usr_segments_1.head(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
      <th>seg_1_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>C971FB21-D54C-4134-938F-16B62EE86D3B</td>
      <td>125</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</td>
      <td>34</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>2</th>
      <td>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</td>
      <td>16</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B7B865AB-0735-407F-8D0C-31F74D2806CC</td>
      <td>7</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E0535076-6270-4DF2-8621-CB06264A94FA</td>
      <td>4</td>
      <td>более двух заказов</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_usr_segments_1.tail(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
      <th>seg_1_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2180</th>
      <td>6E34F9F8-2E96-48D3-A282-66C71A43BF48</td>
      <td>1</td>
      <td>один заказ</td>
    </tr>
    <tr>
      <th>2181</th>
      <td>6DDD4081-515E-401A-9567-8C0EAB6DC868</td>
      <td>1</td>
      <td>один заказ</td>
    </tr>
    <tr>
      <th>2182</th>
      <td>6DBAE21E-4B94-4B16-9548-24AADF146AE2</td>
      <td>1</td>
      <td>один заказ</td>
    </tr>
    <tr>
      <th>2183</th>
      <td>6DA7E5E8-060B-4116-91EF-015B33965D68</td>
      <td>1</td>
      <td>один заказ</td>
    </tr>
    <tr>
      <th>2184</th>
      <td>FFE82299-3F5B-4214-87FE-3D36ECCCFAC3</td>
      <td>1</td>
      <td>один заказ</td>
    </tr>
  </tbody>
</table>
</div>



**Оформляем сегментацию  в виде сводной таблицы с категориями и идентификаторами пользователей.**


```python

df_segment_1 = df_usr_segments_1.pivot_table(\
                                          index=['seg_1_category', 'customer_id'],\
                                          values='orders_counter', aggfunc='sum').sort_values('orders_counter', ascending=False)

df_segment_1.info()
df_segment_1.head()

```

    <class 'pandas.core.frame.DataFrame'>
    MultiIndex: 2185 entries, ('более двух заказов', 'C971FB21-D54C-4134-938F-16B62EE86D3B') to ('один заказ', 'FFE82299-3F5B-4214-87FE-3D36ECCCFAC3')
    Data columns (total 1 columns):
     #   Column          Non-Null Count  Dtype
    ---  ------          --------------  -----
     0   orders_counter  2185 non-null   int64
    dtypes: int64(1)
    memory usage: 40.7+ KB
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>orders_counter</th>
    </tr>
    <tr>
      <th>seg_1_category</th>
      <th>customer_id</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">более двух заказов</th>
      <th>C971FB21-D54C-4134-938F-16B62EE86D3B</th>
      <td>125</td>
    </tr>
    <tr>
      <th>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</th>
      <td>34</td>
    </tr>
    <tr>
      <th>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</th>
      <td>16</td>
    </tr>
    <tr>
      <th>B7B865AB-0735-407F-8D0C-31F74D2806CC</th>
      <td>7</td>
    </tr>
    <tr>
      <th>498F12A4-6A62-4725-8516-CF5DC9AB8A3A</th>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_usr_segments_1.groupby('seg_1_category')['customer_id'].agg('count').sort_values().plot(kind='pie', \
                                                                       fontsize=12, autopct='%1.0f%%', figsize=(10, 9))
None

```


    
![png](output_170_0.png)
    


Распределение пользователей по количеству заказов:  

- 61% клиентов: оформили 1 заказ
- 38% клиентов: оформили 2 заказа
- 1% клиентов: оформили более 2 заказов.  

Сегментация по количеству заказов позволит далее изучить клиентов по сегментам, например, для разработки мероприятий по привлечению/удержанию клиентов.


#### Сегменты по категориям товаров

Разделим всех пользователей на сегменты в зависимости от категории приобретаемых товаров.
Построим сводную таблицу для оценки...


```python

df_seg_product_cat = df.pivot_table(index=['category','customer_id'], values='order_id',\
                                   aggfunc='nunique').sort_values(['category', 'order_id'], ascending=False).reset_index()
df_seg_product_cat.columns = ['category','customer_id', 'orders_counter']
df_seg_product_cat.info()
df_seg_product_cat.head(5)

```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 3059 entries, 0 to 3058
    Data columns (total 3 columns):
     #   Column          Non-Null Count  Dtype 
    ---  ------          --------------  ----- 
     0   category        3059 non-null   object
     1   customer_id     3059 non-null   object
     2   orders_counter  3059 non-null   int64 
    dtypes: int64(1), object(2)
    memory usage: 71.8+ KB
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>category</th>
      <th>customer_id</th>
      <th>orders_counter</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ЦВЕТЫ</td>
      <td>C971FB21-D54C-4134-938F-16B62EE86D3B</td>
      <td>11</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ЦВЕТЫ</td>
      <td>41117D9D-94F7-4145-A8C9-CB6675CE7674</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ЦВЕТЫ</td>
      <td>E8204583-4D55-4724-AD3F-049C7DB43BDD</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ЦВЕТЫ</td>
      <td>01F95D43-4A8D-45E5-9C23-CCCD211E6D30</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ЦВЕТЫ</td>
      <td>080AC19B-4BEB-49D7-B960-5CF01C5638EF</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_cat = df.pivot_table(index=['category'], \
                        values='customer_id', aggfunc='nunique').sort_values('customer_id', ascending=False).\
plot(kind='bar', figsize=(10,4), color='lightblue', legend=False, use_index=True)

plt.title('Количество пользователей по категориям товаров', fontsize=12)
plt.xlabel("категория", fontsize=10)
plt.ylabel("пользователи", fontsize=10)

plt.xticks(rotation=45)
plt.tick_params(length=4)

plt.show()
None

```


    
![png](output_175_0.png)
    


 По количеству клиентов лидируют категории ХОЗТОВАРЫ, ТОВАРЫ ДЛЯ ДАЧИ, ТОВАРЫ ДЛЯ ДОМА и ЦВЕТЫ.  
 С учетом предпочтений отдельных категорий товаров можно использовать эту сегментацию, например, для персонализированных рекламных рассылок.

#### Сегменты по среднему чеку


```python

df.head(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>product</th>
      <th>quantity</th>
      <th>price</th>
      <th>year</th>
      <th>week</th>
      <th>month</th>
      <th>month_name</th>
      <th>day</th>
      <th>week_day</th>
      <th>week_day_name</th>
      <th>hour</th>
      <th>total_price</th>
      <th>product_sstr</th>
      <th>category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ АЛОЕ ВЕРА, D12, H30</td>
      <td>1</td>
      <td>142.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>142.0</td>
      <td>КОМНАТНОЕ</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>КОМНАТНОЕ РАСТЕНИЕ В ГОРШКЕ КОФЕ АРАБИКА, D12, H25</td>
      <td>1</td>
      <td>194.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>194.0</td>
      <td>КОМНАТНОЕ</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>РАДЕРМАХЕРА D-12 СМ H-20 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
      <td>РАДЕРМАХЕРА</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ХРИЗОЛИДОКАРПУС ЛУТЕСЦЕНС D-9 СМ</td>
      <td>1</td>
      <td>179.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>179.0</td>
      <td>ХРИЗОЛИДОКАРПУС</td>
      <td>ЦВЕТЫ</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2018-10-01</td>
      <td>EE47D746-6D2F-4D3C-9622-C31412542920</td>
      <td>68477</td>
      <td>ЦИПЕРУС ЗУМУЛА D-12 СМ H-25 СМ</td>
      <td>1</td>
      <td>112.0</td>
      <td>2018</td>
      <td>40</td>
      <td>10</td>
      <td>October</td>
      <td>1</td>
      <td>0</td>
      <td>Monday</td>
      <td>0</td>
      <td>112.0</td>
      <td>ЦИПЕРУС</td>
      <td>ЦВЕТЫ</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_users_cheque = df.groupby(['customer_id', 'order_id']).agg({'quantity': 'sum', 'total_price': 'sum'}).reset_index()

df_users_cheque.columns = ['customer_id', 'order_id', 'items_counter', 'total_amount']

df_users_cheque.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>order_id</th>
      <th>items_counter</th>
      <th>total_amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>000D6849-084E-4D9F-AC03-37174EAF60C4</td>
      <td>14943</td>
      <td>4</td>
      <td>555.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>001CEE7F-0B29-4716-B202-0042213AB038</td>
      <td>70290</td>
      <td>1</td>
      <td>442.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>00299F34-5385-4D13-9AEA-C80B81658E1B</td>
      <td>72965</td>
      <td>2</td>
      <td>914.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>002D4D3A-4A59-406B-86EC-C3314357E498</td>
      <td>69990</td>
      <td>1</td>
      <td>1649.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>003BBD39-0000-41FF-B7F9-2DDAEC152037</td>
      <td>72796</td>
      <td>2</td>
      <td>2324.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
#---- посчитаем среднее количество товаров в заказе + общую стоимость заказов + среднюю стоимость заказа


df_users_cheque = df_users_cheque.groupby('customer_id').agg({'items_counter': 'mean',\
                                                              'total_amount':['sum', 'mean']}).reset_index()

df_users_cheque.head()


```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th>customer_id</th>
      <th>items_counter</th>
      <th colspan="2" halign="left">total_amount</th>
    </tr>
    <tr>
      <th></th>
      <th></th>
      <th>mean</th>
      <th>sum</th>
      <th>mean</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>000D6849-084E-4D9F-AC03-37174EAF60C4</td>
      <td>4.0</td>
      <td>555.0</td>
      <td>555.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>001CEE7F-0B29-4716-B202-0042213AB038</td>
      <td>1.0</td>
      <td>442.0</td>
      <td>442.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>00299F34-5385-4D13-9AEA-C80B81658E1B</td>
      <td>2.0</td>
      <td>914.0</td>
      <td>914.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>002D4D3A-4A59-406B-86EC-C3314357E498</td>
      <td>1.0</td>
      <td>1649.0</td>
      <td>1649.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>003BBD39-0000-41FF-B7F9-2DDAEC152037</td>
      <td>2.0</td>
      <td>2324.0</td>
      <td>2324.0</td>
    </tr>
  </tbody>
</table>
</div>



Сегменты по среднему чеку можно в дальнейшейм использовать, например,  для персонализированных рекламных рассылок с учетом соответствующего ценового диапазона.

#### Сегменты (RFM-анализ)

**RFM** — это метод, используемый для анализа потребительской ценности, группирует клиентов на основе истории их транзакций:

**Recency (Давность)** — Как давно клиент совершил покупку?  

**Frequency (Частота)** — Как часто они совершают покупки?  

**Monetary Value** (Денежная ценность) — Сколько они тратят?

Максимальная дата в датасете:


```python

max_order_date_value = df['date'].max()
print(max_order_date_value)

```

    2020-01-31 15:00:00
    

Сформируем RFM-таблицу:


```python

df_RFM = df.groupby('customer_id').agg({'date': lambda x: (max_order_date_value - x.max()).days, # Recency
                                        'order_id': lambda x: len(x.unique()), # Frequency
                                        'total_price': lambda x: x.sum()})    # Monetary 

df_RFM['date'] = df_RFM['date'].astype(int)

df_RFM.rename(columns={'date': 'recency', 
                         'order_id': 'frequency',
                         'total_price': 'monetary'}, inplace=True)
df_RFM.head()


```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
    </tr>
    <tr>
      <th>customer_id</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>000D6849-084E-4D9F-AC03-37174EAF60C4</th>
      <td>107</td>
      <td>1</td>
      <td>555.0</td>
    </tr>
    <tr>
      <th>001CEE7F-0B29-4716-B202-0042213AB038</th>
      <td>349</td>
      <td>1</td>
      <td>442.0</td>
    </tr>
    <tr>
      <th>00299F34-5385-4D13-9AEA-C80B81658E1B</th>
      <td>108</td>
      <td>1</td>
      <td>914.0</td>
    </tr>
    <tr>
      <th>002D4D3A-4A59-406B-86EC-C3314357E498</th>
      <td>368</td>
      <td>1</td>
      <td>1649.0</td>
    </tr>
    <tr>
      <th>003BBD39-0000-41FF-B7F9-2DDAEC152037</th>
      <td>123</td>
      <td>1</td>
      <td>2324.0</td>
    </tr>
  </tbody>
</table>
</div>



Для расчета показателя RFM будем использовать квантили.


```python

quantiles = df_RFM.quantile(q=[0.25,0.5,0.75])
quantiles

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0.25</th>
      <td>65.0</td>
      <td>1.0</td>
      <td>402.0</td>
    </tr>
    <tr>
      <th>0.50</th>
      <td>191.0</td>
      <td>1.0</td>
      <td>845.5</td>
    </tr>
    <tr>
      <th>0.75</th>
      <td>338.0</td>
      <td>2.0</td>
      <td>1799.0</td>
    </tr>
  </tbody>
</table>
</div>



Выполним сегментацию и определим RFM-классы (комбинации значений R, F и M).


```python
quantiles = quantiles.to_dict()
quantiles
```




    {'recency': {0.25: 65.0, 0.5: 191.0, 0.75: 338.0},
     'frequency': {0.25: 1.0, 0.5: 1.0, 0.75: 2.0},
     'monetary': {0.25: 402.0, 0.5: 845.5, 0.75: 1799.0}}




```python

##  RFM - сегментация ----

RFM_Segment = df_RFM.copy()

#---------------

def R_Class(x,p,d):
    if x <= d[p][0.25]:
        return 4
    elif x <= d[p][0.50]:
        return 3
    elif x <= d[p][0.75]: 
        return 2
    else:
        return 1
    
#---------------

def FM_Class(x,p,d):
    if x <= d[p][0.25]:
        return 1
    elif x <= d[p][0.50]:
        return 2
    elif x <= d[p][0.75]: 
        return 3
    else:
        return 4
    
RFM_Segment['R_Quartile'] = RFM_Segment['recency'].apply(R_Class, args=('recency',quantiles,))

RFM_Segment['F_Quartile'] = RFM_Segment['frequency'].apply(FM_Class, args=('frequency',quantiles,))

RFM_Segment['M_Quartile'] = RFM_Segment['monetary'].apply(FM_Class, args=('monetary',quantiles,))

RFM_Segment['RFMClass'] = RFM_Segment.R_Quartile.map(str) + RFM_Segment.F_Quartile.map(str) + RFM_Segment.M_Quartile.map(str)


RFM_Segment.info()

RFM_Segment.sort_values('RFMClass', ascending=False).head(15)

```

    <class 'pandas.core.frame.DataFrame'>
    Index: 2185 entries, 000D6849-084E-4D9F-AC03-37174EAF60C4 to FFE82299-3F5B-4214-87FE-3D36ECCCFAC3
    Data columns (total 7 columns):
     #   Column      Non-Null Count  Dtype  
    ---  ------      --------------  -----  
     0   recency     2185 non-null   int32  
     1   frequency   2185 non-null   int64  
     2   monetary    2185 non-null   float64
     3   R_Quartile  2185 non-null   int64  
     4   F_Quartile  2185 non-null   int64  
     5   M_Quartile  2185 non-null   int64  
     6   RFMClass    2185 non-null   object 
    dtypes: float64(1), int32(1), int64(4), object(1)
    memory usage: 128.0+ KB
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
      <th>R_Quartile</th>
      <th>F_Quartile</th>
      <th>M_Quartile</th>
      <th>RFMClass</th>
    </tr>
    <tr>
      <th>customer_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>6F977064-EBCB-45D0-9115-4C641341FCC8</th>
      <td>14</td>
      <td>2</td>
      <td>4731.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>952CD551-78D7-4D6E-815E-D2335CAC37B5</th>
      <td>11</td>
      <td>2</td>
      <td>2570.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>1A32C1A4-5862-4896-8671-FA4C1FFCF0C4</th>
      <td>43</td>
      <td>2</td>
      <td>2605.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>904015BA-31F2-4CE4-B68E-02362280A43D</th>
      <td>2</td>
      <td>2</td>
      <td>1821.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>1AECD6CC-AD83-4136-9124-061918FFBD36</th>
      <td>65</td>
      <td>2</td>
      <td>4349.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>DCA2ED18-F11F-42D8-A4AE-3295950E39FE</th>
      <td>51</td>
      <td>2</td>
      <td>3189.5</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>DC9EEB94-819E-46A0-9F40-28562193626B</th>
      <td>56</td>
      <td>2</td>
      <td>4117.5</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>91AAC803-C8FC-44FD-A1D7-494E16E2AC2D</th>
      <td>24</td>
      <td>2</td>
      <td>5871.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>DC6C2A43-8B28-495D-854B-12C8CC73470E</th>
      <td>51</td>
      <td>2</td>
      <td>2818.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>DB6CAEDB-E231-4ADC-BCD0-E45B8992F3DA</th>
      <td>23</td>
      <td>2</td>
      <td>8203.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>1CD0C18D-5437-400D-9A64-D0F8BA25F596</th>
      <td>62</td>
      <td>2</td>
      <td>3373.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>1D2D5986-042C-446E-9476-547C860AB4F8</th>
      <td>36</td>
      <td>2</td>
      <td>3808.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>5518A551-5C26-466E-9C7A-335058203480</th>
      <td>65</td>
      <td>2</td>
      <td>2961.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>DA3F682D-81CE-41C5-A9BE-C1D07AC743FB</th>
      <td>25</td>
      <td>2</td>
      <td>5173.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>D99D25F1-4017-4FCD-8D29-C580CC695A1A</th>
      <td>0</td>
      <td>2</td>
      <td>1993.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
  </tbody>
</table>
</div>




```python

RFM_Segment.describe()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
      <th>R_Quartile</th>
      <th>F_Quartile</th>
      <th>M_Quartile</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>2185.000000</td>
      <td>2185.000000</td>
      <td>2185.000000</td>
      <td>2185.000000</td>
      <td>2185.000000</td>
      <td>2185.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>207.510755</td>
      <td>1.481007</td>
      <td>1956.077994</td>
      <td>2.503432</td>
      <td>1.791762</td>
      <td>2.496568</td>
    </tr>
    <tr>
      <th>std</th>
      <td>151.074709</td>
      <td>2.802199</td>
      <td>15055.371137</td>
      <td>1.119308</td>
      <td>0.996388</td>
      <td>1.118489</td>
    </tr>
    <tr>
      <th>min</th>
      <td>0.000000</td>
      <td>1.000000</td>
      <td>15.000000</td>
      <td>1.000000</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>65.000000</td>
      <td>1.000000</td>
      <td>402.000000</td>
      <td>2.000000</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>191.000000</td>
      <td>1.000000</td>
      <td>845.500000</td>
      <td>3.000000</td>
      <td>1.000000</td>
      <td>2.000000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>338.000000</td>
      <td>2.000000</td>
      <td>1799.000000</td>
      <td>4.000000</td>
      <td>3.000000</td>
      <td>3.000000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>487.000000</td>
      <td>125.000000</td>
      <td>675000.000000</td>
      <td>4.000000</td>
      <td>4.000000</td>
      <td>4.000000</td>
    </tr>
  </tbody>
</table>
</div>



Далее по полученной RFM-сегментации Заказчик получает возможность по требуемым сегментам изучать клиентов (примеры ряда запросов приведены ниже) и планировать необходимые мероприятия и акции. 

Посмотрим, кто лучшие клиенты (часто бывают, часто покупают, много тратят)?


```python

#RFMClass = 434 (часто бывают,часто покупают, много тратят)
RFM_Segment[RFM_Segment['RFMClass']=='434'].sort_values('monetary', ascending=False).head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
      <th>R_Quartile</th>
      <th>F_Quartile</th>
      <th>M_Quartile</th>
      <th>RFMClass</th>
    </tr>
    <tr>
      <th>customer_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>F279D50F-A508-40B4-BDE5-5CB4A1BE3AD0</th>
      <td>31</td>
      <td>2</td>
      <td>16557.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>B7DFFBD3-810C-4125-889A-454EE093EB73</th>
      <td>8</td>
      <td>2</td>
      <td>10168.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>AC250053-A236-467A-97D2-DDBB9BF4A1BA</th>
      <td>49</td>
      <td>2</td>
      <td>8684.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>B16A52B7-499B-4BE8-9220-19076ED22BC9</th>
      <td>43</td>
      <td>2</td>
      <td>8307.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
    <tr>
      <th>9704352F-7F7F-4FA7-AA1C-64296156A722</th>
      <td>35</td>
      <td>2</td>
      <td>8249.0</td>
      <td>4</td>
      <td>3</td>
      <td>4</td>
      <td>434</td>
    </tr>
  </tbody>
</table>
</div>



Кто лояльные клиенты (часто покупают)?


```python

RFM_Segment[RFM_Segment['F_Quartile'] >= 3 ].sort_values('monetary', ascending=False).head(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
      <th>R_Quartile</th>
      <th>F_Quartile</th>
      <th>M_Quartile</th>
      <th>RFMClass</th>
    </tr>
    <tr>
      <th>customer_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>C971FB21-D54C-4134-938F-16B62EE86D3B</th>
      <td>331</td>
      <td>125</td>
      <td>152028.0</td>
      <td>2</td>
      <td>4</td>
      <td>4</td>
      <td>244</td>
    </tr>
    <tr>
      <th>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</th>
      <td>464</td>
      <td>34</td>
      <td>53728.0</td>
      <td>1</td>
      <td>4</td>
      <td>4</td>
      <td>144</td>
    </tr>
    <tr>
      <th>58A4C3CC-504F-43EA-A74A-BAE19E665552</th>
      <td>381</td>
      <td>2</td>
      <td>53232.0</td>
      <td>1</td>
      <td>3</td>
      <td>4</td>
      <td>134</td>
    </tr>
    <tr>
      <th>498F12A4-6A62-4725-8516-CF5DC9AB8A3A</th>
      <td>286</td>
      <td>4</td>
      <td>41900.0</td>
      <td>2</td>
      <td>4</td>
      <td>4</td>
      <td>244</td>
    </tr>
    <tr>
      <th>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</th>
      <td>92</td>
      <td>16</td>
      <td>21009.0</td>
      <td>3</td>
      <td>4</td>
      <td>4</td>
      <td>344</td>
    </tr>
  </tbody>
</table>
</div>



Какие клиенты находятся на пороге оттока?


```python

RFM_Segment[RFM_Segment['R_Quartile'] <= 2 ].sort_values('monetary', ascending=False).head(5)

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>recency</th>
      <th>frequency</th>
      <th>monetary</th>
      <th>R_Quartile</th>
      <th>F_Quartile</th>
      <th>M_Quartile</th>
      <th>RFMClass</th>
    </tr>
    <tr>
      <th>customer_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>312E9A3E-5FCA-43FF-A6A1-892D2B2D5BA6</th>
      <td>227</td>
      <td>1</td>
      <td>675000.0</td>
      <td>2</td>
      <td>1</td>
      <td>4</td>
      <td>214</td>
    </tr>
    <tr>
      <th>C971FB21-D54C-4134-938F-16B62EE86D3B</th>
      <td>331</td>
      <td>125</td>
      <td>152028.0</td>
      <td>2</td>
      <td>4</td>
      <td>4</td>
      <td>244</td>
    </tr>
    <tr>
      <th>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</th>
      <td>464</td>
      <td>34</td>
      <td>53728.0</td>
      <td>1</td>
      <td>4</td>
      <td>4</td>
      <td>144</td>
    </tr>
    <tr>
      <th>58A4C3CC-504F-43EA-A74A-BAE19E665552</th>
      <td>381</td>
      <td>2</td>
      <td>53232.0</td>
      <td>1</td>
      <td>3</td>
      <td>4</td>
      <td>134</td>
    </tr>
    <tr>
      <th>146CD9BF-A95C-4AFB-915B-5F6684B17444</th>
      <td>234</td>
      <td>1</td>
      <td>49432.0</td>
      <td>2</td>
      <td>1</td>
      <td>4</td>
      <td>214</td>
    </tr>
  </tbody>
</table>
</div>



### Проверка статистических гипотез

#### Проверка гипотезы об отсутствии отличий между долями клиентов, которые совершают только одну покупку, и более одной покупки.

`Нулевая гипотеза:` Доля клиентов, которые совершают только одну покупку и доля клиентов, которые совершают более одной покупки, не различаются.  

`Альтернативная гипотеза:` Доля клиентов, которые совершают только одну покупку и доля клиентов, которые совершают более одной покупки, различаются.


```python

df_usr_segments_1.head()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
      <th>seg_1_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>C971FB21-D54C-4134-938F-16B62EE86D3B</td>
      <td>125</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</td>
      <td>34</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>2</th>
      <td>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</td>
      <td>16</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B7B865AB-0735-407F-8D0C-31F74D2806CC</td>
      <td>7</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E0535076-6270-4DF2-8621-CB06264A94FA</td>
      <td>4</td>
      <td>более двух заказов</td>
    </tr>
  </tbody>
</table>
</div>




```python

df_usr_segments_1.head()
    
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>orders_counter</th>
      <th>seg_1_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>C971FB21-D54C-4134-938F-16B62EE86D3B</td>
      <td>125</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4D93D3F6-8B24-403B-A74B-F5173E40D7DB</td>
      <td>34</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>2</th>
      <td>73D1CD35-5E5F-4629-8CF2-3FDA829D4E58</td>
      <td>16</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>3</th>
      <td>B7B865AB-0735-407F-8D0C-31F74D2806CC</td>
      <td>7</td>
      <td>более двух заказов</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E0535076-6270-4DF2-8621-CB06264A94FA</td>
      <td>4</td>
      <td>более двух заказов</td>
    </tr>
  </tbody>
</table>
</div>




```python

df1 = df_usr_segments_1.query('seg_1_category == "один заказ"')['orders_counter']
df2 = df_usr_segments_1.query('seg_1_category != "один заказ"')['orders_counter']

# критический уровень статистической значимости, если pvalue окажется меньше него, отвергнем гипотезу
alpha = .05 

results = st.ttest_ind(df1, df2, nan_policy ='omit')
    
print('p-значение: ', results.pvalue)
    
if (results.pvalue < alpha):
    print("Отвергаем нулевую гипотезу")
else:
    print("Не получилось отвергнуть нулевую гипотезу")
    
```

    p-значение:  3.262633098384791e-24
    Отвергаем нулевую гипотезу
    

Отвергаем нулевую гипотезу, т.е. доля клиентов, которые совершают только одну покупку и доля клиентов, которые совершают более одной покупки, различаются.

#### Проверка гипотезы об отсутствии отличий ежемесячной выручки в декабре и ноябре

`Нулевая гипотеза:` Ежемесячная выручка в декабре и ноябре не различаются.  

`Альтернативная гипотеза:` Ежемесячная выручка в декабре и в ноябре различаются.


```python

df1 = df.query('month == 12 and year == 2019')['total_price']
df2 = df.query('month == 11 and year == 2019')['total_price']


# критический уровень статистической значимости, если pvalue окажется меньше него, отвергнем гипотезу
alpha = .05 

results = st.ttest_ind(df1, df2, nan_policy ='omit')
    
print('p-значение: ', results.pvalue)
    
if (results.pvalue < alpha):
    print("Отвергаем нулевую гипотезу")
else:
    print("Не получилось отвергнуть нулевую гипотезу")
    
```

    p-значение:  0.07257127459710831
    Не получилось отвергнуть нулевую гипотезу
    

Не отвергаем нулевую гипотезу, т.е. между выручкой в декабре и ноябре отсутствует косвенная взаимосвязь.

#### Проверка гипотезы об отсутствии отличий по выручке между лояльными клиентами и клиентами на грани оттока

`Нулевая гипотеза:` Выручка между лояльными клиентами и клиентами на грани оттока не различаются.  

`Альтернативная гипотеза:` Выручка между лояльными клиентами и клиентами на грани оттока различаются.


```python

df1 = RFM_Segment.query('F_Quartile >= 3')['monetary']
df2 = RFM_Segment.query('F_Quartile <= 2')['monetary']

# критический уровень статистической значимости, если pvalue окажется меньше него, отвергнем гипотезу
alpha = .05 

results = st.ttest_ind(df1, df2, nan_policy ='omit')
    
print('p-значение: ', results.pvalue)
    
if (results.pvalue < alpha):
    print("Отвергаем нулевую гипотезу")
else:
    print("Не получилось отвергнуть нулевую гипотезу")
    
```

    p-значение:  0.5950632845823729
    Не получилось отвергнуть нулевую гипотезу
    

Не отвергаем нулевую гипотезу.

### Презентация

<a href="https://disk.yandex.ru/i/FJAgaTfb2e6Uhg">Презентация</a>

### Выводы


В соответствии с поставленной задачей проведено исследование с целью выявление профилей потребления для интернет-магазина товаров для дома и быта, выполнено сегментирование покупателей по профилю потребления, по результатам подготовлены рекомендации Заказчику для разработки более персонализированных (таргетированных) предложений для покупателей. 

Для проведения исследования предоставлены данные: датасет описывает транзакции интернет-магазина товаров для дома и быта.  


**Основные этапы исследования:**

1. Загрузка и предобработка данных  

    - проверка наличия пропусков;
    - обработка дубликатов;
    - преобразование типов данных;
    - анализ наличия аномалий в данных (проверка на бизнес-правила "1 заказ - 1 клиент" и "1 заказ" - "1 дата").   
    

2. Исследовательский анализ данных

   - Распределение товаров по категориям;
   - Анализ динамики по количеству заказов, по количеству пользователей, расчет среднего чека и т.д.
   
   
3. Сегментация пользователей
      
 - по количеству заказов  
 - по категориям товаров
 - по среднему чеку  
 
 
4. Проверка статистических гипотез  
    
    - Проверка гипотезы об отсутствии отличий между долями клиентов, которые совершают только одну покупку, и более одной покупки.
    - Проверка гипотезы об отсутствии отличий ежемесячной выручки в декабре и ноябре.
       


**По результатам анализа датасета:**  

- всего строк: 7474;  
- пропуски в столбцах отсутствуют;
- преобразование названий столбцов к нижнему регистру и «snake_case» не требуется.  
- необходимо скорректировать тип столбца `date` на *datetime*.

При выполнении проверки бизнес-правила **"1 заказ - 1 клиент"** установлено, что присутствует 29 "проблемных" заказов, что соответствует 89 транзакциям в датасете и составляет порядка 1.2%. Чтобы исключить искажение результатов (смещение статистик) эти заказы удалены из датасета.  


При выполнении проверки бизнес-правила **"1 заказ" - "1 дата"** найдено 256 "проблемных" заказов, что соответствует 2384 транзакциям в датасете и составляет порядка **32.3%, почти 1/3 всех транзакций в датасете**. Учитывая это, **необходимо проинформировать ответственного представителя Заказчика о проблемах в предоставленной выгрузке с данными.**  Чтобы исключить искажение результатов (смещение статистик) для дальнейшего исследования такие заказы удалены из датасета.


По результатам разбивки на категории выделены следующие категории товаров:

  - ТОВАРЫ ДЛЯ ВАННОЙ
  - ЦВЕТЫ
  - ИСКУССТВЕННЫЕ ЦВЕТЫ
  - ТОВАРЫ ДЛЯ ДАЧИ
  - ТОВАРЫ ДЛЯ ДОМА
  - ПОСУДА
  - ХОЗТОВАРЫ
  - НАБОРЫ
  - РАЗНОЕ    
  
  Анализ показывает, что в течение 2019 года наблюдается определенная "сезонность", например: **рост продаж в феврале, марте, апреле, декабре**...что, вероятнее всего, обусловлено покупками подарков к праздникам (23 февраля, 8 марта, Новый год) и подготовкой к дачному сезону (апрель).   
Если сравнить **данные год к году (2018/2019) за октябрь, ноябрь и декабрь, то видно, что продажи в 2019 г. уступают 2018 г. в октябре и декабре, но превышают в ноябре**.  
Существенный рост в январе 2020 г. относительно января 2019 и декабря 2019 может быть обусловлен, например, проведением  в декабре 2019 г. рекламной акции по продвижению продарочных новогодних сертификатов.  
  
  
  
**Выводы по гистограммам датасета**:

 - наблюдается неравномерное распределение по количеству заказов в течение рассматриваемого периода, есть "всплески" и провалы; 
 - вероятно, что разброс по нумерации заказов, обусловлен наличием различных категорий товаров;
 - подавляющее большинство клиентов приобретают малые партии товаров;
 - основная часть покупок - это недорогие товары;
 - наблюдается сезонность в продажах, с провалами в феврале и марте, а также в период летних отпусков (с июня по сентябрь)
 - по дням недели: больше всего продаж - в понедельник, меньше всего  - в субботу;
 - по часам: наблюдается спад продаж в ночные часы (примерно с 2-х часов до 6).
 - основная часть заказов в датасете - это заказы за 2019 г.  
 
 
 Анализ по дням недели показывает **спад продаж по субботам**. Наиболее активные продажи в общем за период наблюдаются в понедельник и вторник. При этом внутри месяца (на примере данных за январь 2020 г) активность по дням недели может различаться от месяца к месяцу.  
 
 При анализе динамики выручки наблюдается всплеск в июне 2019 г, когда были закуплены крупные оптовые партии отдельных товаров. А также заметен спад, начиная с июля 2019 и до конца 2019 года.
  
По динамике привлечения новых пользователей в лидерах категории: `ХОЗТОВАРЫ` и `ТОВАРЫ ДЛЯ ДАЧИ`.  

**Сегментация пользователей**  


  Анализ показывает, что большая часть пользователей оформили 1 или 2 заказа. Поэтому по количеству сделанных заказов целесообразно разделить всех клиентов на 3 категории:   
  
 - 'один заказ'
 - 'два заказа'
 - 'более двух заказов'.  
 
 
 По долям распределение пользователей по количеству заказов выглядит следующим образом:  

- 61% оформили 1 заказ  
- 38% оформили 2 заказа
- 1% оформили более 2 заказов.  

Сегментация по количеству заказов позволит далее изучить клиентов по сегментам, например, для разработки мероприятий по привлечению/удержанию клиентов.  

В сегментации по товарам: по количеству клиентов лидируют категории ХОЗТОВАРЫ, ТОВАРЫ ДЛЯ ДАЧИ, ТОВАРЫ ДЛЯ ДОМА и ЦВЕТЫ.  
 С учетом предпочтений отдельных категорий товаров можно использовать эту сегментацию, например, для персонализированных рекламных рассылок.  
 
 Сегменты по среднему чеку можно в дальнейшейм использовать, например,  для персонализированных рекламных рассылок с учетом соответствующего ценового диапазона.

По полученной RFM-сегментации Заказчик получает возможность по требуемым сегментам изучать клиентов и планировать необходимые мероприятия и акции. 

  


```python

```
