from datetime import date

from ir.s_datetime import Interval
from ir.SNode import SNode


def main():
    interval = Interval(date(2000,1,1),date(2010,1,1))
    node = SNode(1, 'OBJECT', 'person', interval)
    print(node.node_id)

if __name__ == '__main__':
    main()
