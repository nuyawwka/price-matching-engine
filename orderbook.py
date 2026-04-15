from decimal     import Decimal as D
from itertools   import count
from collections import namedtuple
from datetime    import datetime, timedelta


del_min    = 100   
del_thresh = 1000
del_ratio  = 0.1


class PseudoClock(object):
    def __init__(self, start_time, seconds=1):
        self.start_time = start_time
        self.seconds = seconds
        self.eod = start_time.replace(hour=16, minute=0, second=0, microsecond=0)

    def now(self):
        self.start_time += timedelta(seconds=self.seconds)
        return self.start_time


class Node(object):
    def __init__(self, data):
        self.data = data
        self.prev = None
        self.next = None
    
#------------------------------------------------------------------------------#
# had to roll my own linked list class since deque doesn't support O(1) delete #
#------------------------------------------------------------------------------#

class DoubleLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None

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

#------------------------------------------------------------------------------#
# direct addressing (cpu cache friendly) wins over sorteddict (space friendly) #
#------------------------------------------------------------------------------#

class DirectAddressList(object):
    def __init__(self, desc=False):
        self.desc = desc
        self.base = None
        self.best = None
        self.dels = 0
        self.buf  = []

    def __getitem__(self, key):
        i = self.key_to_idx(key)
        return self.buf[i]

    def __bool__(self):
        return self.best is not None
    
    def key_to_idx(self, key):
        if self.base is None:
            self.base = key
        return int((key - self.base) * D(100))

    def peek(self):
        return (self.best, self[self.best].head.data)
    
    def level(self, key):
        i = self.key_to_idx(key)
        if i >= len(self.buf):
            self.buf.extend([None] * (i - len(self.buf) + 1))
        elif i < 0:
            self.base = key
            self.buf[:0] =  [None] * -i
            i = 0
        if self.desc:
            self.best = max(key, self.best or key)
        else:
            self.best = min(key, self.best or key) 
        if not self.buf[i]: 
            self.buf[i] = DoubleLinkedList()
        return self.buf[i]

    def delete(self, key):
        i = self.key_to_idx(key)
        self.buf[i] = None
        if key == self.best:
            for j in range(i - 1, -1, -1) if self.desc else range(i + 1, len(self.buf)):
                if self.buf[j]:
                    self.best = self.buf[j].head.data.price
                    break
                self.dels += 1
            else:
                self.best = None

        #------------------#
        # lazy compression #
        #------------------#

        if len(self.buf) > del_min and self.dels > del_thresh and (self.dels / len(self.buf)) > del_ratio:
            self.dels = 0
            self.compress()

    def compress(self):
        beg = 0
        end = len(self.buf) - 1
        while beg <= end and self.buf[beg] is None:
            beg += 1
        while end >= beg and self.buf[end] is None:
            end -= 1
        self.buf = self.buf[beg:end + 1]
        if self.buf:
            self.base = self.buf[0].head.data.price
        else:
            self.base = None
    

class Order(object):
    _oid = count(1)

    def __init__(self, order_book, qty, price, expiry):
        self.oid = next(Order._oid)
        self.status = 'OPEN'
        self.order_book = order_book
        self.qty1 = qty
        self.qty2 = qty
        self.price = price
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
            if not isinstance(price, D) or price.as_tuple().exponent != -2:
                raise ValueError('price must be type decimal to 2 places')
            super().__init__(order_book, qty, price, expiry)
            self.type = 'LIMIT'
            self.base = bid_or_ask
            self.order_book.node_map[self.oid] = Node(self)
            level = self.side.level(self.price)
            level.append(self.order_book.node_map[self.oid])

        def delete(self):
            level = self.side.level(self.price)
            level.delete(self.order_book.node_map[self.oid])
            if not level.head:
                self.side.delete(self.price)

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
        self.asks = DirectAddressList()
        self.bids = DirectAddressList(desc=True)
        self.events = {}
        self.node_map = {}
        self.clock = PseudoClock(start_time=start_time or datetime.now())
        self.nt = namedtuple('event', ['seq', 'price', 'qty', 'bidID', 'bidRemaining', 'askID', 'askRemaining', 'time'])

    def cancel(self, order):
        order.status = 'CANCELLED'
        order.delete()

    def submit_order(self, bid_or_ask, qty, price=None, expiry=None):
        order = factory(self, bid_or_ask, qty, price, expiry)
        self.match_order(order, self.clock.now())        
        return order

    def match_order(self, taker, now): 
        while True:
            if taker.expiry < now:
                taker.status = 'EXPIRED'
                taker.delete()
                break
            if not taker.contra:
                break
            price, maker = taker.contra.peek()
            if taker.type != 'MARKET' and taker.price_cmp(price):
                break
            if maker.expiry < now:
                maker.status = 'EXPIRED'
                maker.delete()
                continue
            seq = next(OrderBook._seq)
            num = min(taker.qty2, maker.qty2)
            taker.qty2 -= num
            maker.qty2 -= num
            maker.seqs.append(seq)
            taker.seqs.append(seq)
            self.events[seq] = self.nt(seq, price, num, taker.oid, taker.qty2, maker.oid, maker.qty2, now)
            if maker.qty2 <= 0:
                maker.status = 'FILLED'
                maker.delete()
            else:
                maker.status = 'PARTIAL_FILL'
            if taker.qty2 <= 0:
                taker.status = 'FILLED'
                taker.delete()
                break
            else:
                taker.status = 'PARTIAL_FILL'

#----------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    obook  = OrderBook('AAPL', start_time=datetime(2026, 4, 1))
    order1 = obook.submit_order(bid_or_ask='BID', qty=150, price=D('100.55'))
    order2 = obook.submit_order(bid_or_ask='BID', qty=200, price=D('100.55'))
    order3 = obook.submit_order(bid_or_ask='ASK', qty=125, price=D('100.25'))
    order4 = obook.submit_order(bid_or_ask='BID', qty=100, price=D('100.55'))
    order5 = obook.submit_order(bid_or_ask='ASK', qty=150, price=None)
    order6 = obook.submit_order(bid_or_ask='ASK', qty=175, price=D('101.00'))
    order7 = obook.submit_order(bid_or_ask='ASK', qty=175, price=D('100.00'))

    for o in (order1, order2, order3, order4, order5, order6, order7):
        print('order %d, %s.%s, %s' % (o.oid, o.type, o.base, o.status))
        for seq in o.seqs:
            print('\t', obook.events[seq])
            
