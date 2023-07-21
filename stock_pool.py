import backtrader as bt
from backtrader.feeds import GenericCSVData
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import sys
import logging

class GenericCVS_extend(GenericCSVData):

    # 定义新指标indx并将指标的列输入
    lines = ('indx',)

    params = (('indx', 10),
              ('nullvalue', float('NaN')))


class TotalValue(bt.analyzers.Analyzer):
    """
    Analyzer returning cash and market values
    """

    def create_analysis(self):
        self.rets = {}
        self.vals = 0.0

    def notify_cashvalue(self, cash, value):
        self.vals = (
            self.strategy.datetime.datetime().strftime('%Y-%m-%d'),
            value,
        )
        self.rets[len(self)] = self.vals

    def get_analysis(self):
        return self.rets
    


class MyStrategy(bt.Strategy):
    # 策略参数
    params = dict(
        group = 0,
        printlog=True,
        hedge = False,
        reverse = False,
        total_trade = 0,
        win_count = 0,
        logger = None,
        base = 0,
        total_trend = [],
        account_value = []
    )

    def __init__(self):
        self.indx = dict()
        # 遍历所有股票,提取indx指标
        for data in self.datas:
            self.indx[data._name] = data.indx

        # 建立timer在每周1、2、3、4、5交易
        self.add_timer(
            when = bt.Timer.SESSION_START,
            weekdays = [1],
            monthcarry = True
        )

    # 在timer被触发时调仓
    def notify_timer(self, timer, when, *args, **kwargs):
        self.rebalance_portfolio()
        bond_list = []
        for data in self.datas:
            bond_list.append(data.close[0])
        self.params.total_trend.append(np.mean(bond_list))

    def rebalance_portfolio(self):
        # print('Current time: {}'.format(self.datas[0].datetime.datetime()))

        # 提取股票池当日因子截面
        self.p.account_value.append(self.broker.getvalue())
        rate_list=[]
        for data in self.datas:
            if not self.indx[data._name] == 0:
                rate_list.append([data._name, self.indx[data._name]])

        # 股票池按照因子大小排序，记录前10%的股票，如果因子值为NaN则跳过当个交易日
        long_list=[]
        sorted_rate=sorted(rate_list,key=lambda x:x[1], reverse=self.p.reverse)

        if np.isnan(sorted_rate[0][1][0]):
            return
        group_start = int(len(sorted_rate)/10)*self.params.group
        group_end = int(len(sorted_rate)/10)*(self.params.group+1)
        long_list=[i[0] for i in sorted_rate[group_start:group_end]]

        # for stock in sorted_rate:
        #     print(stock[0], stock[1][0])
        # print("Long_List", long_list)

        if self.p.hedge:
            short_list=[]
            short_list=[i[0] for i in sorted_rate[len(sorted_rate)-int(len(sorted_rate)/10):]]


        # 得到当前的账户价值
        total_value = self.broker.getvalue()
        p_value = total_value*0.5/len(long_list)
        for data in self.datas:
            #获取仓位
            pos = self.getposition(data).size

            if pos!=0 and data._name not in long_list:
                self.close(data = data)
            if not pos and data._name in long_list:
                size=int(p_value/100/data.close[0])*100
                self.buy(data=data, size=size)

            if self.p.hedge and data._name in short_list:
                size=int(p_value/100/data.close[0])*100
                self.sell(data = data, size = size)



    def log(self, txt, dt=None,doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            self.params.logger.info(f'{dt.isoformat()},{txt}')
            print(f'{dt.isoformat()},{txt}')

    #记录交易执行情况（可省略，默认不输出结果）
    def notify_order(self, order):
        # 如果order为submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入:{order.data._name}\n价格:{order.executed.price:.2f},\
                成本:{order.executed.value:.2f},\
                手续费:{order.executed.comm:.2f}')

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'卖出:{order.data._name}\n价格:{order.executed.price:.2f},\
                成本: {order.executed.value:.2f},\
                手续费{order.executed.comm:.2f}')

            self.bar_executed = len(self)

        # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'交易失败:{order.data._name}, 原因:{order.status}, Value: {order.created.value}, Cash: {self.broker.getcash()}')
        self.order = None

    #记录交易收益情况（可省略，默认不输出结果）
    def notify_trade(self,trade):
        if trade.isclosed:
            self.p.total_trade = self.p.total_trade+1
            self.log(f'策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}')
            if trade.pnlcomm > 0:
                self.p.win_count = self.p.win_count+1

    def stop(self):
        if not self.p.printlog:
            print(f'策略胜率：{self.p.win_count/self.p.total_trade}%')
        self.log(f'策略胜率：{self.p.win_count/self.p.total_trade}%')
        return


def backTest(name, save, group, dir, logger, prob):
    cerebro = bt.Cerebro()

    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤
    cerebro.addanalyzer(TotalValue, _name="_TotalValue") # 账户总值

    cerebro.addstrategy(MyStrategy, group=group, logger=logger)

    # 将买卖数量固定为100的倍数
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    # 将文件夹下每一个CSV数据导入策略模型
    data_Dir = 'C:/Users/21995/Desktop/量化投资/CB_Data_Test'
    files = os.listdir(data_Dir)
    for ind, file in enumerate(files):
        if np.random.rand() > prob:
            continue
        data = GenericCVS_extend(
            dataname=data_Dir+'/'+file,
            fromdate=bt.datetime.datetime(2022, 1, 4),
            todate=bt.datetime.datetime(2022, 12, 31),
            dtformat = '%Y-%m-%d %H:%M:%S',
            datetime=2,
            high=4,
            low=5,
            close=6,
            volume=7,
            open=3,
            openinterest = -1,
            indx = 10,
            plot=False
        )

        cerebro.adddata(data)

        print("\r", end="")
        print("Loading data: {}%: ".format(int(ind+1)*100//len(files)), "▋" * (int((int(ind+1)/len(files)) * 100 // 2)), end="")
        sys.stdout.flush()
    print('')

    # 将初始本金设为100w
    cerebro.broker.setcash(1000000.0)
    #cerebro.broker.setcommission(0.005)
    #cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.TimeReturn)

    print('启动资金: %.2f' % cerebro.broker.getvalue())

    result = cerebro.run()

    print('期末价值: %.2f' % cerebro.broker.getvalue())

    
    account_value = np.array(result[0].p.account_value)
    account_value = account_value/(account_value[0]/2)-1
    total_trend = np.array(result[0].p.total_trend)
    total_trend = total_trend/total_trend[0]

    df = pd.DataFrame(data={'Market trend':total_trend, f"{name} Return":account_value})

    if save and os.path.exists(f"{dir}.csv"):
        temp_df = pd.read_csv(f"{dir}.csv")
        temp_df[f"{name} Return"] = df[f"{name} Return"]
        temp_df.to_csv(f"{dir}.csv", mode='w', index = False)
    elif save:
        df.to_csv(f"{dir}.csv", index=False)
    else:
        cerebro.plot()
        plt.plot(account_value, label="Account value")
        plt.plot(total_trend, label="Market trend")
        plt.title("Account value compared to market trend")
        plt.legend()
        plt.show()


if __name__ == '__main__':
    logging.basicConfig(filename=f'backTest.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)
    backTest(name=f"top {0*10} to {0*10+10}%", save=False, group=0, dir='', logger=logger, prob=1)
    # for a in range(0,10):
    # print(f"top {a*10} to {a*10+10}%")
    # backTest(name=f"top {a*10} to {a*10+10}%", save=True, group=a, dir="Result_FluxRate", logger=logger, prob=1)