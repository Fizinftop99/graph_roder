import numpy as np
import pandas as pd
from neo4j import GraphDatabase


def get_gesns(file_name: str) -> list:
    gesn_arr = pd.read_excel(file_name)['Смета'].to_numpy()
    gesns = []
    group = []
    for s in gesn_arr:
        if s.startswith('('):
            group.append(s[s.find('(') + 1: s.find(')')])
        elif group:  # list 'group' is not empty
            gesns.append(np.array(group))
            group = []
    return gesns


def get_all_id():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
    session = driver.session(database='new')
    q_data_obtain = f'''
        MATCH (n)

        RETURN n
        '''

    result = session.run(q_data_obtain).data()
    id_lst = []
    for i in result:
        id_lst.append((i['n']['id']))

    return list(set(id_lst))


def main():
    lst = get_gesns('data/График_ЕКС_по_объекту_Мосфильмовская.xlsx')
    print(len(lst))
    for ind, item in enumerate(lst):
        print(ind, item)


if __name__ == "__main__":
    main()
