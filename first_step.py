from neo4j import GraphDatabase


def first_step():
    id_lst = ['R1870',  # Подъем каркаса
              'R1040',  # Каркас установлен
              'R1000',  # Договор заключен
              'R1080',  # Разработка Проектной документации
              ]
    id_str = ', '.join(map(lambda x: "'" + x + "'", id_lst))
    q_data_obtain = f'''
    MATCH (n)-[]->(m)
    WHERE n.id IN [{id_str}] AND m.id IN [{id_str}]
    RETURN n,m
    '''
    driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
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


if __name__ == "__main__":
    first_step()
