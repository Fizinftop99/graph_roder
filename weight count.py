from neo4j import GraphDatabase, Transaction
import pandas as pd


# noinspection PyArgumentList
def read_graph_data(file_name: str) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    graph_data = df[['Идентификатор операции', 'Название операции', 'Последователи']]  # , 'Предшественники'
    # graph_data['Идентификатор операции'] = graph_data['Идентификатор операции'].apply(str.strip)
    graph_data.loc[:, 'Идентификатор операции'] = graph_data['Идентификатор операции'].apply(str.strip)
    graph_data = graph_data[graph_data['Идентификатор операции'].str.startswith('R')]
    graph_data.set_index('Идентификатор операции', inplace=True)  # Update indeces
    return graph_data


def make_graph(tx: Transaction, data: pd.DataFrame):
    id_lst = data.index.tolist()
    for wrk_id in id_lst:
        tx.run("MERGE (a:Work {id: $id, name: $name})", id=wrk_id,
               name=data.loc[wrk_id, 'Название операции'])
        # name=wrk_id)

        s = data.loc[wrk_id, 'Последователи']
        if s == s:  # Проверка на то, что есть Последователи (s != NaN)
            followers = s.split(', ')
            for flw_id in followers:
                if flw_id in id_lst:  # Создаем ребра только к вершинам, описанным в таблице отдельной строкой
                    tx.run("MATCH (a:Work) WHERE a.id = $wrk_id "
                           "MERGE (follower:Work {id: $flw_id, name: $flw_name}) "
                           "MERGE (a)-[r:FOLLOWS]->(follower) "
                           "SET r.weight = coalesce(r.weight, 0) + 1",
                           wrk_id=wrk_id, flw_id=flw_id,
                           flw_name=data.loc[flw_id, 'Название операции'])  # if flw_id in id_lst else flw_id)


def clear_database(tx: Transaction):
    tx.run("MATCH (n) "
           "DETACH DELETE n")


def main():
    data = read_graph_data("2021-11-19 Roder связи.xlsx")
    # data = read_graph_data("1.xlsx")
    # data2 = read_graph_data("2.xlsx")
    # data3 = read_graph_data("3.xlsx")
    # data4 = read_graph_data("4.xlsx")
    # data5 = read_graph_data("5.xlsx")
    # data6 = read_graph_data("6.xlsx")
    # data7 = read_graph_data("7.xlsx")
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Accelerati0n"))
    # driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    with driver.session() as session:
        session.write_transaction(clear_database)
        session.write_transaction(make_graph, data)
        # session.write_transaction(make_graph, data2)
        # session.write_transaction(make_graph, data3)
        # session.write_transaction(make_graph, data4)
        # session.write_transaction(make_graph, data5)
        # session.write_transaction(make_graph, data6)
        # session.write_transaction(make_graph, data7)
    driver.close()


if __name__ == "__main__":
    main()
