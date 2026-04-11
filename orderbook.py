from   sortedcontainers import SortedDict
from   decimal          import Decimal as D
from   itertools        import count
from   collections      import namedtuple
from   datetime         import datetime, timedelta


class PseudoClock(object):
    def __init__(self, start_time, seconds=1):
        self.start_time = start_time
        self.seconds = seconds
        self.eod = start_time.replace(hour=16, minute=0, second=0, microsecond=0)

    def now(self):
        self.start_time += timedelta(seconds=self.seconds)
        return self.start_time


class DefaultSortedDict(SortedDict):
    def __init__(self, default=None, reverse=False, *args, **kwargs):
        if reverse: args = (lambda k: -k, *args)
        super().__init__(*args, **kwargs)
        self.default = default

    def __getitem__(self, key):
        if key not in self:
            if not self.default: raise KeyError(key)
            self[key] = self.default()
        return super().__getitem__(key)
    

class Node(object):
    def __init__(self, data):
        self.data = data
        self.prev = None
        self.next = None
    

class LinkedList:
    #-----------------------------------#
    # custom linkedlist needed for O(1) #
    # deletions                         #
    #-----------------------------------#
    def __init__(self):
        self.head = None
        self.tail = None
    #-----------------------------------#
    # convenience for display/debugging #
    #-----------------------------------#
    def __iter__(self):
        node = self.head
        while node:
            yield node.data
            node = node.next

    def append(self, node):
        if self.head is None:
            self.head = node
            self.tail = node
            node.prev = None
            node.next = None
        else:
            self.tail.next = node
            node.prev = self.tail
            node.next = None
            self.tail = node

    def delete(self, node):
        if not node.prev and not node.next:
            self.head = None
            self.tail = None
        elif not node.prev:
            self.head = node.next
            self.head.prev = None
        elif not node.next:
            self.tail = node.prev
            self.tail.next = None
        else:
            node.prev.next = node.next
            node.next.prev = node.prev


class Order(object):
    _oid = count(1)

    def __init__(self, order_book, qty, price, expiry):
        self.oid = next(Order._oid)
        self.status = ['CREATED']
        self.order_book = order_book
        self.qty1 = qty
        self.qty2 = qty
        self.price = price
        self.created = order_book.clock.now()
        self.expiry = expiry or order_book.clock.eod
        self.seqs = []


class Bid(Order):
    def __init__(self, *args, **argv):
        super().__init__(*args, **argv)
        self.side = self.order_book.bids
        self.contra = self.order_book.asks

    def price_cmp(self, price):
        return price > self.price


class Ask(Order):
    def __init__(self, *args, **argv):
        super().__init__(*args, **argv)
        self.side = self.order_book.asks
        self.contra = self.order_book.bids

    def price_cmp(self, price):
        return price < self.price


def factory(order_book, bid_or_ask, qty, price=None, expiry=None):
    class Limit(Ask if bid_or_ask == 'ASK' else Bid):
        def __init__(self):
            super().__init__(order_book, qty, price, expiry)
            self.type = 'LIMIT'
            self.base = bid_or_ask
            self.order_book.node_map[self.oid] = Node(self)
            self.side[self.price].append(self.order_book.node_map[self.oid])

        def delete(self):
            self.side[self.price].delete(self.order_book.node_map[self.oid])
            if not self.side[self.price].head:
                del self.side[self.price]
        
    class Market(Ask if bid_or_ask == 'ASK' else Bid):
        def __init__(self):
            super().__init__(order_book, qty, price, expiry)
            self.type = 'MARKET'
            self.base = bid_or_ask

        #-------------------------------------------------------------------#
        # market orders do not live in book - dummy delete simplifies logic #
        #-------------------------------------------------------------------#

        def delete(self):
            pass

    return Limit() if price else Market()


class OrderBook(object):
    _seq = count(1)

    def __init__(self, ticker, start_time=None):
        self.ticker = ticker
        self.asks = DefaultSortedDict(default=LinkedList)
        self.bids = DefaultSortedDict(default=LinkedList, reverse=True)
        self.audit = {}
        self.node_map = {}
        self.clock = PseudoClock(start_time=start_time or datetime.now())
        self.nt = namedtuple('fill', ['price', 'qty', 'bidID', 'bidRemaining', 'askID', 'askRemaining', 'time'])

    def cancel_order(self, order, sys=False):
        order.status.append('SYS_CANCELLED' if sys else 'USER_CANCELLED')
        order.delete()

    def submit_order(self, bid_or_ask, qty, price=None, expiry=None):
        order = factory(self, bid_or_ask, qty, price, expiry)
        self.match_order(order)
        if order.type == 'MARKET' and order.status[-1] != 'FILLED':
            self.cancel_order(order, sys=True)         
        return order

    def match_order(self, taker): 
        while True:
            now = self.clock.now()
            if taker.expiry < now:
                taker.status.append('EXPIRED')
                taker.delete()
                break
            if not taker.contra:
                break
            price, orders = taker.contra.peekitem(0)
            if taker.type != 'MARKET' and taker.price_cmp(price):
                break
            maker = orders.head.data
            if maker.expiry < now:
                maker.status.append('EXPIRED')
                maker.delete()
                continue
            seq = next(OrderBook._seq)
            num = min(taker.qty2, maker.qty2)
            taker.qty2 -= num
            maker.qty2 -= num
            maker.seqs.append(seq)
            taker.seqs.append(seq)
            self.audit[seq] = self.nt(price, num, taker.oid, taker.qty2, maker.oid, maker.qty2, now)
            if maker.qty2 <= 0:
                maker.status.append('FILLED')
                maker.delete()
            else:
                maker.status.append('PARTIAL_FILL')
            if taker.qty2 <= 0:
                taker.status.append('FILLED')
                taker.delete()
                break
            else:
                taker.status.append('PARTIAL_FILL')

#----------------------------------------------------------------------------

if __name__ == '__main__':
    obook  = OrderBook('AAPL', start_time=datetime(2026, 4, 1))
    order1 = obook.submit_order(bid_or_ask='BID', qty=150, price=D('100.55'))
    order2 = obook.submit_order(bid_or_ask='BID', qty=200, price=D('100.55'))
    order3 = obook.submit_order(bid_or_ask='BID', qty=100, price=D('100.55'), expiry=datetime(2026, 3, 31))
    obook.cancel_order(order2)
    order4 = obook.submit_order(bid_or_ask='ASK', qty=125, price=D('100.25'))
    order5 = obook.submit_order(bid_or_ask='ASK', qty=325, price=None)

    for o in (order1, order2, order3, order4, order5):
        print('order %d, %s.%s, %s' % (o.oid, o.type, o.base, o.status))
        for seq in o.seqs:
            print(obook.audit[seq])
