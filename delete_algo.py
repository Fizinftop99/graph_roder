from datetime import datetime
import numpy as np
from neo4j import GraphDatabase
from data_process import get_gesns
from graph_creation import create_graph

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
session = driver.session(database='bfs')


def simple_merging(start_id: str, end_id: str):
    session.run('''
                MERGE (n:Work {id: $id1})
                MERGE (m:Work {id: $id2})
                MERGE (n)-[r:FOLLOWS]->(m)
                ''',
                id1=start_id,
                id2=end_id
                )


def smart_merging(start_id: list, end_id: list):
    session.run(f'''
                MERGE (n)-[r:FOLLOWS]->(m)
                WHERE n.id IN [{start_id}] AND n.id IN [{end_id}]
                ''',
                id1=start_id,
                id2=end_id
                )


def removing_node(id: str):
    income_data_obtain = f'''
            MATCH (n)-[]->(m)
            WHERE m.id = $id
            RETURN n
            '''
    outcome_data_obtain = f'''
                MATCH (n)-[]->(m)
                WHERE n.id = $id
                RETURN m
                '''
    incoming = session.run(income_data_obtain, id=id).data()
    outcoming = session.run(outcome_data_obtain, id=id).data()
    # преобразование результатов запроса в numpy.array
    incoming = np.array([row['n']['id'] for row in incoming])
    outcoming = np.array([row['m']['id'] for row in outcoming])

    for element in incoming:
        for subelement in outcoming:
            simple_merging(element, subelement)

    session.run("MATCH (n) WHERE n.id = $id DETACH DELETE n", id=id)

    print(incoming, outcoming)


def removing_nodes(red_ids: list, ids: list):
    for element in ids:
        if element not in red_ids:
            removing_node(element)


def get_all_id():
    q_data_obtain = '''
        MATCH (n)
        RETURN n
        '''
    result = session.run(q_data_obtain).data()
    id_lst = []
    for i in result:
        id_lst.append((i['n']['id']))
    return list(set(id_lst))


def main():
    # create_graph()
    # gesns = get_gesns('data/График_ЕКС_по_объекту_Мосфильмовская.xlsx')
    starttime = datetime.now()
    # removing_nodes(sec.id_from_new_db(), fs.id_from_new_db())
    removing_nodes(['1', '3', '4', '9', '10', '11', '12', '13'], get_all_id())
    print(datetime.now() - starttime)
    driver.close()


if __name__ == "__main__":
    main()
