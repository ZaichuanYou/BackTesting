import backtrader as bt
from backtrader.feeds import GenericCSVData
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import sys
import logging
from math import sqrt
from scipy.stats import rankdata

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
        hedge=False,
        reverse = False,
        total_trade = 0,
        win_count = 0,
        rankIC = [],
        rankICIR = [],
        logger = None,
        base = 0,
        total_trend = [],
        last_day_data = [],
        last_day_close = {},
        account_value = [],
        index_mean = [],
        index_mean_long = [],
        index_mean_short = []
    )

    def __init__(self):
        # 建立timer在每周1、2、3、4、5交易

        self.indx = dict()
        # 遍历所有股票,提取indx指标
        for data in self.datas:
            self.indx[data._name] = data.indx

        self.add_timer(
            when = bt.Timer.SESSION_START,
            weekdays = [1,2,3,4,5],
            monthcarry = True
        )

    # 在timer被触发时调仓
    def notify_timer(self, timer, when, *args, **kwargs):
        self.rebalance_portfolio()


    def rebalance_portfolio(self):

        if not len(self.p.last_day_data)==0:
            # Calculate the RankIC of index
            # convert to ranks
            self.p.last_day_data = np.array(self.p.last_day_data)
            last_index_list = self.p.last_day_data[:,2]
            today_close = []
            for data in self.datas:
                if data._name in self.p.last_day_data[:,0]:
                    today_close.append(data.close[0])
            corr = pd.DataFrame({"index":rankdata(last_index_list, method='dense'), 
                                 "return":rankdata((np.array(today_close)-self.p.last_day_data[:,1].astype(np.float64))/self.p.last_day_data[:,1].astype(np.float64),
                                  method='dense')})
            # calculate the correlation
            correlation = corr["index"].corr(corr["return"])
            self.p.rankIC.append(correlation)
            self.p.rankICIR.append(correlation*sqrt(len(last_index_list)))
            self.log(f"Total correlation: {correlation}")
            self.log(f"Total rankICIR: {correlation*sqrt(len(last_index_list))}")


        # 计算因子胜率
        win_count = 0
        total_count = 0
        if not len(self.p.last_day_data)==0:
            for data in self.datas:
                pos = self.getposition(data).size
                if pos > 0:
                    if data.close[0]-self.p.last_day_close[data._name]>=0:
                        win_count = win_count+1
                    total_count = total_count+1
                elif pos < 0:
                    if data.close[0]-self.p.last_day_close[data._name]<0:
                        win_count = win_count+1
                    total_count = total_count+1
        self.p.total_trade = self.p.total_trade+total_count
        self.p.win_count = self.p.win_count+win_count
        

        # 提取股票池当日因子截面
        # 计算当日整个市场走势
        self.p.account_value.append(self.broker.getvalue())
        self.p.last_day_data = []
        rate_list=[]
        bond_list=[]
        index_mean = 0
        for data in self.datas:
            if (not self.indx[data._name] == 0) & (not np.isnan(self.indx[data._name][0])):
                rate_list.append([data._name, data.indx[0]])
                index_mean = index_mean+self.indx[data._name][0]
                self.p.last_day_data.append([data._name, data.close[0], data.indx[0]])
            bond_list.append(data.close[0])
        self.params.total_trend.append(np.mean(bond_list))
        
        # 如果当时没有因子则跳过该交易日
        if len(rate_list) == 0:
            return

        # 计算当时全市场因子平均值
        self.log(f'index mean: {index_mean/len(rate_list)}')
        self.p.index_mean.append(index_mean/len(rate_list))

        # 股票池按照因子大小排序
        long_list=[]
        sorted_rate=sorted(rate_list,key=lambda x:x[1], reverse=self.p.reverse)
        
        # 选择对应的可转债
        group_start = int(len(sorted_rate)/10)*self.params.group
        group_end = int(len(sorted_rate)/10)*(self.params.group+1)
        long_list=[i[0] for i in sorted_rate[group_start:group_end]]

        # 如果进行多空对冲则再记录空头可转债
        if self.p.hedge:
            short_list=[]
            short_list=[i[0] for i in sorted_rate[-len(long_list):]]


        # 得到当前的账户价值
        total_value = self.broker.getvalue()
        p_value = total_value*0.5/len(long_list)
        
        # 根据因子进行平仓开仓并记录多空因子平均值
        count_long = 0
        count_short = 0
        close_long = 0
        close_short = 0
        new_long = 0
        new_short = 0
        index_mean_long = 0
        index_mean_short = 0
        self.p.last_day_close = {}
        for data in self.datas:
            if data._name in long_list:
                index_mean_long = index_mean_long+self.indx[data._name][0]
                self.p.last_day_close[data._name] = data.close[0]
            elif self.p.hedge and data._name in short_list:
                index_mean_short = index_mean_short+self.indx[data._name][0]
                self.p.last_day_close[data._name] = data.close[0]

            #获取仓位
            pos = self.getposition(data).size

            if pos>0: count_long = count_long+1
            elif pos<0: count_short = count_short+1

            if pos>0 and data._name not in long_list:
                self.close(data = data)
                close_long = close_long+1
            if self.p.hedge and pos<0 and data._name not in short_list:
                self.close(data = data)
                close_short = close_short+1

            if not pos>0 and data._name in long_list:
                size=int(p_value/data.close[0])
                if size==0:self.log("ERROR")
                new_long = new_long+1
                self.buy(data=data, size=size)

            if self.p.hedge and not pos<0 and data._name in short_list:
                size=int(p_value/data.close[0])
                self.sell(data=data, size=size)
                new_short = new_short+1
        self.p.index_mean_long.append(index_mean_long/len(long_list))

        # 如果进行多空对冲记录空头因子平均值
        # 记录多空情况以及多空新开
        if self.p.hedge:
            if index_mean_short/len(short_list)>10:
                self.log("Large Index!!")
                self.log(short_list)
            self.p.index_mean_short.append(index_mean_short/len(short_list))
            self.log(f"Number of Short: {count_short}, List length: {len(short_list)}, close short: {close_short}, new_short: {new_short}")
        self.log(f"Number of Long: {count_long}, List length: {len(long_list)}, close long: {close_long}, new long: {new_long}")
    

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
            # self.p.total_trade = self.p.total_trade+1
            self.log(f'策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}')
            # if trade.pnlcomm > 0:
            #     self.p.win_count = self.p.win_count+1

    def stop(self):
        if not self.p.printlog:
            print(f'策略胜率：{self.p.win_count/self.p.total_trade}%')
        self.log(f'策略胜率：{self.p.win_count/self.p.total_trade}%')
        self.log(f'Index rankIC: {np.mean(self.p.rankIC)}')
        self.log(f'Index rankICIR: {np.mean(self.p.rankICIR)}')
        self.log(f"Index IR: {np.mean(self.p.rankIC)/np.std(self.p.rankIC)}")
        return