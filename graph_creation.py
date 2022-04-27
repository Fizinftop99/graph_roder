import numpy as np
from neo4j import GraphDatabase, Transaction
import pandas as pd


def read_graph_data(file_name: str) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    df.drop_duplicates(keep='first', inplace=True)
    graph_data = df[['Идентификатор операции', 'ADCM_шифрГЭСН', 'Последователи']]
    graph_data.loc[:, 'Идентификатор операции'] = graph_data['Идентификатор операции'].apply(str.strip)
    graph_data = graph_data[graph_data['Идентификатор операции'].str.startswith('A')]
    graph_data.set_index('Идентификатор операции', inplace=True)  # Update indeces
    return graph_data


def make_graph(tx: Transaction, data: pd.DataFrame):
    id_lst = data.index.to_numpy()
    for wrk_id in id_lst:
        wrk_gesn = data.loc[wrk_id, 'ADCM_шифрГЭСН']
        a = wrk_gesn.split('-')
        chap, vol = a[0].split('.')
        work_type = a[1]
        work = a[2]
        tx.run("MERGE (a:Work {id: $id, name: $name, chapter: $chap, volume: $vol, work_type: $wrk_t, work: $work})",
               id=wrk_gesn, name=wrk_gesn, chap=chap, vol=vol, wrk_t=work_type, work=work)

        s = data.loc[wrk_id, 'Последователи']
        if s == s:  # Проверка на то, что есть Последователи (s != NaN)
            followers = np.array(s.split(', '))
            for flw_id in np.intersect1d(followers, id_lst):
                flw_gesn = data.loc[flw_id, 'ADCM_шифрГЭСН']
                if flw_gesn != wrk_gesn:
                    a = flw_gesn.split('-')
                    flw_chap, flw_vol = a[0].split('.')
                    flw_work_type = a[1]
                    flw_work = a[2]
                    tx.run('''MATCH (a:Work) WHERE a.id = $wrk_id
                           MERGE (f:Work {id: $flw_id, name: $flw_name, 
                           chapter: $chap, volume: $vol, work_type: $wrk_t, work: $work})
                           MERGE (a)-[r:FOLLOWS]->(f)
                           SET r.weight = coalesce(r.weight, 0) + 1''',
                           wrk_id=wrk_gesn,
                           flw_id=flw_gesn,
                           flw_name=flw_gesn,
                           chap=flw_chap,
                           vol=flw_vol,
                           wrk_t=flw_work_type,
                           work=flw_work
                           )


def add(tx: Transaction, node: str, edges: list):
    a = node.split('-')
    chap, vol = a[0].split('.')
    work_type = a[1]
    work = a[2]
    tx.run("MERGE (a:Work {id: $id, name: $name, chapter: $chap, volume: $vol, work_type: $wrk_t, work: $work})",
           id=node, name=node, chap=chap, vol=vol, wrk_t=work_type, work=work)
    for i in edges:
        tx.run('''MATCH (a:Work) WHERE a.id = $wrk_id
               MATCH (f:Work) WHERE f.id = $flw_id
               MERGE (a)-[r:FOLLOWS]->(f)
               SET r.weight = coalesce(r.weight, 0) + 1''',
               wrk_id=node, flw_id=i)


def clear_database(tx: Transaction):
    tx.run('''MATCH (n)
           DETACH DELETE n''')


def create_graph():
    # df = read_graph_data("data/2022-02-07 МОЭК_ЕКС график по смете.xlsx")
    # df.drop_duplicates(keep='first', inplace=True)
    # driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))

    # driver = GraphDatabase.driver("neo4j+s://174cd36c.databases.neo4j.io:7687", auth=("neo4j",
    # "w21V4bw-6kTp9hceHMbnlt5L9X1M4upuuq2nD7tD_xU"))
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
    with driver.session(database='new') as session:
        # session.write_transaction(clear_database)
        # session.write_transaction(make_graph, df)
        session.write_transaction(add, '50.50-50-50', ['3.6-8-1', '3.1-29-1', '6.68-84-4'])
    driver.close()


if __name__ == "__main__":
    create_graph()
