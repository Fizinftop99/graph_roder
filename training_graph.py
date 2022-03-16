"""
Create a graph for testing algorithms
"""
from neo4j import GraphDatabase, Transaction


def add_nodes(tx: Transaction, k: int):
    q_create = 'CREATE (a:Work {id: $id, name: $id})'
    for i in range(1, k+1):
        tx.run(q_create, id=str(i))


def add_edge(tx: Transaction, n: int, m: int):
    q_edge = '''
    MATCH (a:Work) WHERE a.id = $id1 
    MATCH (b:Work) WHERE b.id = $id2
    MERGE (a)-[r:FOLLOWS]->(b)
    '''
    tx.run(q_edge, id1=str(n), id2=str(m))


def main():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))
    # driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    with driver.session(database="bfs") as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.write_transaction(add_nodes, 14)
        session.write_transaction(add_edge, 1, 2)
        session.write_transaction(add_edge, 1, 3)
        session.write_transaction(add_edge, 1, 4)
        session.write_transaction(add_edge, 2, 5)
        session.write_transaction(add_edge, 2, 6)
        session.write_transaction(add_edge, 3, 6)
        session.write_transaction(add_edge, 3, 7)
        session.write_transaction(add_edge, 4, 8)
        session.write_transaction(add_edge, 5, 11)
        session.write_transaction(add_edge, 6, 11)
        session.write_transaction(add_edge, 5, 9)
        session.write_transaction(add_edge, 6, 9)
        session.write_transaction(add_edge, 7, 9)
        session.write_transaction(add_edge, 7, 10)
        session.write_transaction(add_edge, 8, 10)
        session.write_transaction(add_edge, 8, 13)
        session.write_transaction(add_edge, 9, 12)
        session.write_transaction(add_edge, 10, 12)
        session.write_transaction(add_edge, 11, 14)
        session.write_transaction(add_edge, 12, 14)
        session.write_transaction(add_edge, 13, 14)
    driver.close()


if __name__ == "__main__":
    main()
