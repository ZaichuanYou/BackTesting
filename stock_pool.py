from backtraderHelpers import *

def backTest(name, save, group, dir, logger, prob=1):
    """
        Conduct the backtest using given parameters

        params:
            name: name of current group
            save: save the result to a csv file or not
            group: current group
            dir: directory which will be use to save the data
            logger: the universal logger
            prob: default is one, fraction of data that will be accept by the program
    """
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
            todate=bt.datetime.datetime(2023, 7, 14),
            dtformat = '%Y-%m-%d %H:%M:%S',
            datetime=1,
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
    index_mean = result[0].p.index_mean
    index_mean_long = result[0].p.index_mean_long
    if result[0].p.hedge:
        index_mean_short = result[0].p.index_mean_short
    total_trend = np.array(result[0].p.total_trend)
    total_trend = total_trend/total_trend[0]

    df = pd.DataFrame(data={'Market trend':total_trend, f"{name} Return":account_value})

    if save and os.path.exists(f"{dir}.csv"):
        temp_df = pd.read_csv(f"{dir}.csv")
        temp_df[f"{name} Return"] = df[f"{name} Return"].tail(int(len(account_value)/(group+1))).values
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
        plt.plot(index_mean, label="Market index mean")
        plt.plot(index_mean_long, label="Long index mean")
        if result[0].p.hedge:
            plt.plot(index_mean_short, label="Short index mean")
        plt.title("Index mean of the whole market over time")
        plt.legend()
        plt.show()


if __name__ == '__main__':
    logging.basicConfig(filename=f'backTest.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)
    backTest(name=f"top {0*10} to {0*10+10}%", save=False, group=0, dir='Result', logger=logger, prob=1)
    # for a in range(0,10):
    #     print(f"top {a*10} to {a*10+10}%")
    #     backTest(name=f"top {a*10} to {a*10+10}%", save=True, group=a, dir="Result", logger=logger, prob=1)