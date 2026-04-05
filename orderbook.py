from   sortedcontainers import SortedDict
from   decimal          import Decimal as D
from   itertools        import count
from   collections      import namedtuple
from   datetime         import datetime, timedelta
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PseudoClock(object):
    def __init__(self, start_time, inc=1):
        self.start_time = start_time
        self.inc = inc
        self.eod = start_time.replace(hour=16, minute=0, second=0, microsecond=0)

    def now(self):
        self.start_time += timedelta(minutes=self.inc)
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
        self.status = 'ACTIVE'
        self.order_book = order_book
        self.qty1 = qty
        self.qty2 = qty
        self.price = price
        self.created = order_book.clock.now()
        self.expiry = expiry or order_book.clock.eod

    def __str__(self):
        return str({k:v for k,v in self.__dict__.items() if k != 'side'})


class BidSide(Order):
    def __init__(self, *args, **argv):
        super().__init__(*args, **argv)
        self.side = self.order_book.bids
        self.contra = self.order_book.asks

    def price_cmp(self, price):
        return price > self.price


class AskSide(Order):
    def __init__(self, *args, **argv):
        super().__init__(*args, **argv)
        self.side = self.order_book.asks
        self.contra = self.order_book.bids

    def price_cmp(self, price):
        return price < self.price


def factory(order_book, bid_or_ask, qty, price=None, expiry=None):
    class LimOrder(AskSide if bid_or_ask == 'ASK' else BidSide):
        def __init__(self):
            super().__init__(order_book, qty, price, expiry)
            self.type = 'LIMIT'
            self.order_book.node_map[self.oid] = Node(self)
            self.side[self.price].append(self.order_book.node_map[self.oid])

        def delete(self):
            self.side[self.price].delete(self.order_book.node_map[self.oid])
            if not self.side[self.price].head:
                del self.side[self.price]
        
    class MktOrder(AskSide if bid_or_ask == 'ASK' else BidSide):
        def __init__(self):
            super().__init__(order_book, qty, price, expiry)
            self.type = 'MARKET'

        #-------------------------------------------------------------------#
        # market orders do not live in book - dummy delete simplifies logic #
        #-------------------------------------------------------------------#

        def delete(self):
            pass

    return LimOrder() if price else MktOrder()


class OrderBook(object):
    _seq = count(1)

    def __init__(self, ticker, start_time=None, verbose=False):
        self.ticker = ticker
        self.verbose = verbose
        self.asks = DefaultSortedDict(default=LinkedList)
        self.bids = DefaultSortedDict(default=LinkedList, reverse=True)
        self.audit = {}
        self.node_map = {}
        self.clock = PseudoClock(start_time=start_time or datetime.now())
        self.nt = namedtuple('fill', ['seq', 'price', 'qty', 'bidID', 'bidRemaining', 'askID', 'askRemaining', 'time'])

    @staticmethod
    def cancel_order(order):
        order.status = 'CANCELLED'
        order.delete()

    def submit_order(self, bid_or_ask, qty, price=None, expiry=None):
        order = factory(self, bid_or_ask, qty, price, expiry)
        self.match_order(order)
        if order.status == 'ACTIVE' and order.type == 'MARKET':
            self.cancel_order(order)         
        if self.verbose:
            logger.info(type(order).__name__, order)
            logger.info([val for _, val in sorted(self.audit.items())])
        return order

    def match_order(self, taker): 
        while True:
            now = self.clock.now()
            if taker.expiry < now:
                taker.status = 'EXPIRED'
                taker.delete()
                break
            if not taker.contra:
                break
            price, orders = taker.contra.peekitem(0)
            if taker.type != 'MARKET' and taker.price_cmp(price):
                break
            maker = orders.head.data
            if maker.expiry < now:
                maker.status = 'EXPIRED'
                maker.delete()
                continue
            seq = next(OrderBook._seq)
            num = min(taker.qty2, maker.qty2)
            taker.qty2 -= num
            maker.qty2 -= num
            self.audit[seq] = self.nt(seq, price, num, taker.oid, taker.qty2, maker.oid, maker.qty2, now)
            if maker.qty2 <= 0:
                maker.status = 'FILLED'
                maker.delete()
            if taker.qty2 <= 0:
                taker.status = 'FILLED'
                taker.delete()
                break

#-----------------------------------------------------------------------

if __name__ == '__main__':
    obook  = OrderBook('AAPL', start_time=datetime(2026, 4, 1), verbose=True)
    order1 = obook.submit_order(bid_or_ask='BID', qty=150, price=D('100.55'))
    order2 = obook.submit_order(bid_or_ask='BID', qty=200, price=D('100.55'))
    order3 = obook.submit_order(bid_or_ask='BID', qty=100, price=D('100.55'), expiry=datetime(2026, 3, 31))
    order4 = obook.submit_order(bid_or_ask='ASK', qty=125, price=D('100.25'))
    order5 = obook.submit_order(bid_or_ask='ASK', qty=325, price=None)
    status = obook.cancel_order(order2)
