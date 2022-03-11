from neo4j import GraphDatabase


# получаем все id из новой бд

def id_from_new_db():
    # q_data_obtain = f'''
    #     MATCH (n)-[]->()
    #
    #     RETURN n
    #     '''
    #
    # driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    # session = driver.session(database="neo4j")#new
    # result = session.run(q_data_obtain).data()
    # id_lst = []
    # for i in result:
    #     id_lst.append((i['n']['id']))
    #
    # return list(set(id_lst))
    id_lst = ['R1870',  # Подъем каркаса
              'R1040',  # Каркас установлен
              'R1000',  # Договор заключен
              'R1080',  # Разработка Проектной документации
              'R1190'  # Установка опорных плит
              ]
    return id_lst


# поиск ближайших друзей для элемента

def first_friends(id):
    driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    session1 = driver.session(database="neo4j")

    id_lst = [str(id)]

    id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
        MATCH (n)-[]->(m)
        WHERE n.id IN [{id_str}]
        RETURN m
        '''

    result = session1.run(q_data_obtain).data()
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
    driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    session = driver.session(database="neo4j")

    # id_lst = [str(id)]
    #
    # id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
            MATCH (n)-[]->(m)
            WHERE n.id = $id
            RETURN m
            '''

    result = session.run(q_data_obtain, id=id).data()
    # print(id)
    # если нет наследников то вернуть резульат
    if not result:
        print(level)
        print(search_result)
        return search_result

    # если глубина результата меньше чем, глубина поиска, то добавляем список со значением глубины
    if len(search_result) < level:
        search_result.append(list())
        search_result[level - 1] = [level, []]

    for i in result:
        # если элемент красный, то добавляем его в результат
        if i['m']['id'] in id_list and i['m']['id'] not in search_result[level - 1][1]:
            search_result[level - 1][1].append((i['m']['id']))
        # если элемент не красный, то рекурсивно продолжаем поиск
        else:
            deep_search(i['m']['id'], id_list, search_result, level + 1)

    return search_result


def new_search(id_list):
    for element in id_list:
        print("Начинаем работу с новым id")
        deep_search(element, id_list)
        # запускаем поиск в глубину до первого красного


def merging(el_id, nodes):
    # Remote database
    # driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))

    # Local database:
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Accelerati0n"))

    session = driver.session(database="new")
    for node in nodes:
        for level in node[1]:  # level = list of friends ids
            if el_id in id_from_new_db() and level in id_from_new_db():
                session.run('''
                            MERGE (n:Work {id: $id1})
                            MERGE (m:Work {id: $id2})
                            MERGE (n)-[r:FOLLOWS]->(m)
                            ''',
                            id1=el_id,
                            id2=level
                            )


def main():
    ids = id_from_new_db()
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Accelerati0n"))
    with driver.session(database="new") as session:
        session.run("MATCH (n) DETACH DELETE n")

    # for element in ids:
    #     # print(element, friend(element, 1))
    #     merging(element, friend(element, 1))  # глубина поиска
    for element in ids:
        # print(element, deep_search(element, ids))
        search_result = []
        print("Работаем с новым id")
        merging(element, deep_search(element, ids, search_result))


if __name__ == "__main__":
    main()
