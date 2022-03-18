from datetime import datetime
import numpy as np
from neo4j import GraphDatabase
import second_step as sec
import fast_step as fs


driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
session = driver.session()


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
    incoming = np.array([r['n']['id'] for r in incoming])
    outcoming = np.array([r['m']['id'] for r in outcoming])

    for element in incoming:
        for subelement in outcoming:
            simple_merging(element, subelement)

    session.run("MATCH (n) WHERE n.id = $id DETACH DELETE n", id=id)

    print(incoming, outcoming)


def removing_nodes(red_ids: list, ids: list):
    for element in ids:
        if element not in red_ids:
            removing_node(element)


def main():
    starttime = datetime.now()
    removing_nodes(sec.id_from_new_db(), fs.id_from_new_db())
    print(datetime.now() - starttime)
    driver.close()


if __name__ == "__main__":
    main()
