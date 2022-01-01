from neo4j import GraphDatabase, Transaction
import pandas as pd


def read_graph_data(file_name: str) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    graph_data = df[['Идентификатор операции', 'Название операции', 'Предшественники', 'Последователи']]
    graph_data['Идентификатор операции'] = graph_data['Идентификатор операции'].apply(str.strip)
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
                # if flw_id == "R1010":
                #     print(wrk_id, "->", flw_id)
                tx.run("MATCH (a:Work) WHERE a.id = $wrk_id "
                       "MERGE (follower:Work {id: $flw_id, name: $flw_name}) "
                       "MERGE (a)-[:FOLLOWS]->(follower)",
                       wrk_id=wrk_id, flw_id=flw_id,
                       flw_name=data.loc[flw_id, 'Название операции'])  # if flw_id in id_lst else flw_id)


def main():
    data = read_graph_data("2021-11-19 Roder связи.xlsx")
    # data = read_graph_data("R1080.xlsx")
    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "2310"))
    with driver.session() as session:
        session.write_transaction(make_graph, data)
        # session.write_transaction(add_friend, "Arthur", "Guinevere")
        # session.write_transaction(add_friend, "Arthur", "Lancelot")
        # session.write_transaction(add_friend, "Arthur", "Merlin")
        # session.read_transaction(print_friends, "Arthur")
    driver.close()


if __name__ == "__main__":
    main()
