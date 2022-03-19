from neo4j import GraphDatabase, Transaction  # , Transaction


# получаем все id из новой бд
def test_id():
    id_lst = ['R1870',  # Подъем каркаса
              'R1040',  # Каркас установлен
              'R1000',  # Договор заключен
              'R1080',  # Разработка Проектной документации
              'R1190'  # Установка опорных плит
              ]
    return id_lst


def id_from_new_db():
    q_data_obtain = f'''
        MATCH (n)
        
        RETURN n
        '''

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Accelerati0n"))
    session = driver.session(database="neo4j")
    result = session.run(q_data_obtain).data()
    id_lst = []
    for i in result:
        id_lst.append((i['n']['id']))

    return list(set(id_lst))


# поиск ближайших друзей для элемента

def first_friends(id: str):
    driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    session1 = driver.session(database="neo4j")

    # q_data_obtain_1 = f'''
    #             MATCH (n)-[]->(m)
    #
    #             RETURN n
    #             '''
    #
    # result = session1.run(q_data_obtain_1).data()

    id_lst = [str(id)]

    # print(result)
    # id_lst = ['R1870',  # Подъем каркаса
    #           # 'R1040',  # Каркас установлен
    #           # 'R1000',  # Договор заключен
    #           # 'R1080',  # Разработка Проектной документации
    #           ]

    id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
        MATCH (n)-[]->(m)
        WHERE n.id IN [{id_str}]
        RETURN m
        '''

    # session2 = driver.session(database="new")

    result = session1.run(q_data_obtain).data()
    for i in result:
        id_lst.append((i['m']['id']))
    if id in id_lst:
        id_lst.remove(id)
    return id_lst


# для друзей ищем их друзей и запомниаем их их глубину относительно нулевого элемента


def second_friends(id: str):
    friends = first_friends(id)
    result = []
    for element in friends:
        result.extend(first_friends(element))

    if id in result:
        result.remove(id)

    return result


# смотрим как они связаны в старом графе

def friend(id: str, n: int):
    result = first_friends(id)
    super_result = [(0, first_friends(id))]
    new_result = []
    for i in range(1, n):
        for element in result:
            new_result.append(second_friends(element))
        new_result = [value for value in new_result if value][0]
        super_result.append((i, new_result))
        result = new_result
        new_result = []
    return super_result


def merging(tx: Transaction, el_id: str, nodes: list):
    # session = driver.session(database="new")
    for node in nodes:
        for level in node[1]:  # level = list of friends ids
            for id in level:
                tx.run('''
                            MERGE (n:Work {id: $id1})
                            MERGE (m:Work {id: $id2})
                            MERGE (n)-[r:FOLLOWS]->(m)
                            ''',
                       id1=el_id,
                       id2=id
                       )


# super_result = []
# # print(first_friends('R1090'))
# # super_result.append((0, first_friends('R1090')))
# # print(second_friends('R1090'))
# # print(friend('R1090', 3))
# print(id_from_new_db())
#
# for element in id_from_new_db():
#     print(element, friend(element, 1))
#     merging(friend(element, 1))


def main():
    # Remote database
    # driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))

    # Local database:
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
    ids = id_from_new_db()
    print(ids)
    with driver.session(database="neo4j") as session:
        session.run("MATCH (n) DETACH DELETE n")
        for element in ids:
            print(element, friend(element, 1))
            session.write_transaction(merging, element, friend(element, 1))
    driver.close()


if __name__ == "__main__":
    main()
