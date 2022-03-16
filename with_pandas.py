from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import datetime

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))


# получаем все id из новой бд
def id_from_new_db():
    id_lst = ['1', '3', '4', '9', '10', '11', '12', '13']
    return id_lst


# поиск ближайших друзей для элемента

def first_friends(id):
    session = driver.session(database="bfs")

    id_lst = [str(id)]

    id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
        MATCH (n)-[]->(m)
        WHERE n.id IN [{id_str}]
        RETURN m
        '''

    result = session.run(q_data_obtain).data()
    for i in result:
        id_lst.append((i['m']['id']))
    if id in id_lst:
        id_lst.remove(id)
    return id_lst


# для друзей ищем их друзей и запомниаем их их глубину относительно нулевого элемента
def second_friends(id):
    friends = first_friends(id)
    result = []
    for element in friends:
        result.extend(first_friends(element))

    if id in result:
        result.remove(id)

    return result


# смотрим как они связаны в старом графе
# переписать функцию так, чтобы искала, пока
# не встретит либо другой красный элемент, либо тупик, либо не зайдет слишком глубоко
def friend(id, n):
    result = first_friends(id)
    super_result = [(0, first_friends(id))]
    new_result = []
    for i in range(1, n):
        for element in result:
            new_result.append(second_friends(element))
        if new_result:
            new_result = [value for value in new_result if value][0]
            super_result.append((i, set(new_result).intersection(set(id_from_new_db()))))
        result = new_result
        new_result = []
    return super_result


def deep_search(id, id_list, search_result, level=1, iter=1):
    session = driver.session(database="bfs")
    q_data_obtain = f'''
            MATCH (n)-[]->(m)
            WHERE n.id = $id
            RETURN m
            '''
    result = session.run(q_data_obtain, id=id).data()

    # если нет наследников то вернуть результат
    if not result:
        return search_result

    df = pd.DataFrame([r['m'] for r in result])
    # если глубина результата меньше чем глубина поиска, то добавляем список со значением глубины
    if len(search_result) < level:
        search_result.append(list())
        search_result[level - 1] = [level, []]

    target_df = df.loc[df['id'].isin(id_list)]
    not_target_df = df.loc[~df['id'].isin(id_list)]
    search_result[level - 1][1].extend(target_df['id'].tolist())
    for i in not_target_df['id'].tolist():
        deep_search(i, id_list, search_result, level + 1)

    return search_result


def merging(el_id, nodes):
    session = driver.session(database="new")
    for node in nodes:
        for level in node[1]:  # level = list of friends ids
            if el_id in id_from_new_db() and level in id_from_new_db():
                session.run('''
                            MERGE (n:Work {id: $id1, name: $id1})
                            MERGE (m:Work {id: $id2, name: $id2})
                            MERGE (n)-[r:FOLLOWS]->(m)
                            ''',
                            id1=el_id,
                            id2=level
                            )


def main():
    starttime = datetime.datetime.now()
    ids = id_from_new_db()
    driver.session(database="new").run("MATCH (n) DETACH DELETE n")

    for element in ids:
        search_result = []
        merging(element, deep_search(element, ids, search_result))

    print(datetime.datetime.now() - starttime)


if __name__ == "__main__":
    main()
