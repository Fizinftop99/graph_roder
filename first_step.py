from neo4j import GraphDatabase


def add_nodes(id_lst: list[str]):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
    id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
        MATCH (n)
        WHERE n.id IN [{id_str}]
        RETURN n
        '''
    session1 = driver.session(database="neo4j")
    session2 = driver.session(database="new")
    result = session1.run(q_data_obtain).data()
    for i in result:
        session2.run('MERGE (n:Work {id: $id1, name: $name1})',
                     name1=i['n']['name'],
                     id1=i['n']['id']
                     )
    driver.close()


def add_edges(id_lst: list[str]):
    id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
    MATCH (n)-[]->(m)
    WHERE n.id IN [{id_str}] AND m.id IN [{id_str}]
    RETURN n,m
    '''
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
    session1 = driver.session(database="neo4j")
    session2 = driver.session(database="new")
    result = session1.run(q_data_obtain).data()
    for i in result:
        session2.run('''
                    MERGE (n:Work {id: $id1, name: $name1})
                    MERGE (m:Work {id: $id2, name: $name2})
                    MERGE (n)-[r:FOLLOWS]->(m)
                    ''',
                     name1=i['n']['name'],
                     name2=i['m']['name'],
                     id1=i['n']['id'],
                     id2=i['m']['id']
                     )
    driver.close()


def main():
    id_lst = ['R1870',  # Подъем каркаса
              'R1040',  # Каркас установлен
              'R1000',  # Договор заключен
              'R1080',  # Разработка Проектной документации
              'R1190'  # Установка опорных плит
              ]
    add_nodes(id_lst)


if __name__ == "__main__":
    main()
