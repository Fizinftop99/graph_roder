import pandas as pd
from neo4j import GraphDatabase
import re


def read_graph_data(file_name: str) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    df.drop_duplicates(keep='first', inplace=True)
    graph_data = df[['Идентификатор операции', 'ADCM_шифрГЭСН', 'Последователи']]
    graph_data['Идентификатор операции'] = graph_data['Идентификатор операции'].apply(str.strip)
    graph_data = graph_data[graph_data['Идентификатор операции'].str.startswith('A')]
    graph_data.set_index('Идентификатор операции', inplace=True)  # Update indeces
    return graph_data


def m():
    df_init = pd.read_excel("2022-02-07 МОЭК_ЕКС график по смете.xlsx")
    df_init["is_duplicate"] = df_init.duplicated()
    df = read_graph_data("2022-02-07 МОЭК_ЕКС график по смете.xlsx")
    df["is_duplicate"] = df.duplicated()
    with pd.ExcelWriter('duplicated.xlsx', mode='w') as excel_output1:
        df_init.to_excel(excel_output1, sheet_name='before')
        df.to_excel(excel_output1, sheet_name='after')


def main():
    s = '(Learn Python) (not C++)'
    result = re.findall('\(.*?\)', s)
    print(result)

    s = 'Learn Python (not C++)'
    s = 'dffvdd'
    result = s[s.find('(') + 1:s.find(')')]
    print(s.find('('))


if __name__ == '__main__':
    main()
