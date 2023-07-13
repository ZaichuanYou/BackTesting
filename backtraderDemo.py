import backtrader as bt
import backtrader.feeds as btfeed
import pandas as pd
import matplotlib.pyplot as plt

class TestStrategy(bt.Strategy):
    def log(self, txt, dt=None, doprint=False):
        if doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):

        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.sma5 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=5)
        self.sma10 = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=10)

    def notify_order(self, order):

        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):

        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm), doprint=True)

    def next(self):

        self.log('Close, %.2f' % self.dataclose[0])

        if self.order:
            return

        if not self.position:
            if self.sma5[0] > self.sma10[0]:
                self.order = self.buy()
        else:
            if self.sma5[0] < self.sma10[0]:
                self.order = self.sell()

    def stop(self):
        self.log(u'(金叉死叉有用吗) Ending Value %.2f' %
                 (self.broker.getvalue()), doprint=True)

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤

    strats = cerebro.addstrategy(TestStrategy)

    cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    data = bt.feeds.GenericCSVData(
        dataname='C:/Users/21995/Desktop/量化投资/数据/SZ000001 行情.csv',
        fromdate=bt.datetime.datetime(2014, 4, 1),
        todate=bt.datetime.datetime(2015, 4, 30),
        dtformat = '%Y-%m-%d',
        datetime=0,
        high=1,
        low=2,
        close=4,
        volume=5,
        open=3,
        openinterest = -1
    )

    cerebro.adddata(data)

    cerebro.broker.setcash(1000000.0)
    cerebro.broker.setcommission(0.005)

    print('启动资金: %.2f' % cerebro.broker.getvalue())
    
    result = cerebro.run()

    strat = result[0]
    daily_return = pd.Series(strat.analyzers.pnl.get_analysis())
    
    cerebro.plot()
    # plt.plot(daily_return)
    # plt.show()