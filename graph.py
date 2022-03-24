import link
import node


class Graph(object):
    def __init__(self, name="", nodes: list = [], links: list = []):
        self.name = name
        self.nodes = nodes
        self.links = links

    def copy(self, example: object):
        self.name = example.name
        self.nodes = example.nodes
        self.links = example.links

    def clean(self):
        self.__init__()

    @property
    def number_of_nodes(self):
        return len(self.nodes)

    @property
    def number_of_links(self):
        return len(self.links)

    def __iter__(self):
        self.cur = -1
        return self

    def __next__(self):
        if self.number_of_nodes - 1 >= self.iter_pos:
            self.cur += 1
            return self.nodes[self.iter_pos]
        else:
            raise StopIteration
