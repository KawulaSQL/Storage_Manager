class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.values = []  # Stores values directly in sorted order
        self.children = []  # Children nodes
        self.next = None  # Pointer to the next leaf node (for range queries)


class BPlusTree:
    def __init__(self, order=4):
        """
        Initializes a B+ Tree.
        :param order: Maximum number of children per internal node.
        """
        self.order = order
        self.root = BPlusTreeNode(is_leaf=True)

    