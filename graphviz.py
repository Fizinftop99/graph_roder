import pandas as pd
from graphviz import Digraph


def read_graph_data(file_name: str) -> pd.DataFrame:
    df = pd.read_excel(file_name)  # , index_col='Идентификатор операции')
    graph_data = df[['Идентификатор операции', 'Название операции', 'Предшественники', 'Последователи']]
    graph_data['Идентификатор операции'] = graph_data['Идентификатор операции'].apply(str.strip)
    graph_data = graph_data[graph_data['Идентификатор операции'].str.startswith('R')]
    graph_data.set_index('Идентификатор операции', inplace=True)  # Update indeces
    return graph_data


def make_graph(data: pd.DataFrame) -> Digraph:
    id_lst = data.index.tolist()
    g_attr = {
        # 'lheight': '20',
        'size': '10,5',
        'ratio': 'fill'
    }
    n_attr = {
        # 'shape': 'egg',
        # 'align': 'left',
        # 'fontsize': '20',
        # 'ranksep': '0.1',
        'height': '1'
    }
    # gra = Digraph(graph_attr=g_attr, node_attr=n_attr)
    gra = Digraph()
    for i in id_lst:
        gra.node(i, data.loc[i, 'Название операции'])
        s = data.loc[i, 'Последователи']
        if s == s:  # Проверка на то, что есть Последователи (s != NaN)
            followers = s.split(', ')
            for j in followers:
                gra.edge(i, j)
    return gra


def main():
    # data1 = read_graph_data('1.xlsx')
    # gra1 = make_graph(data1)
    # gra1.render('Graph1', view=True)
    #
    # data2 = read_graph_data('2.xlsx')
    # gra2 = make_graph(data2)
    # gra2.render('Graph2', view=True)
    #
    # data3 = pd.concat([data1, data2])
    # gra3 = make_graph(data3)
    # gra3.render('Graph3', view=True)

    data3 = read_graph_data('data/2021-11-19 Roder связи.xlsx')
    gra3 = make_graph(data3)
    gra3.render('Graph3')


if __name__ == "__main__":
    main()
