import delete_algo


class Node(object):

    def __init__(self, id="", name="", label=[]):
        self.id = id
        self.name = name
        self.label = label

    def copy(self, example: object):
        self.id = example.id
        self.name = example.name
        self.label = example.label

    def __delete__(self, instance):
        delete_algo.removing_node(self.id)
        self.id = ""
        self.name = ""
        self.label = []
