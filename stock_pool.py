from backtraderHelpers import *
import pathlib

def backTest(name, save, group, data_dir, result_dir, logger, prob=1):
    """
        Conduct the backtest using given parameters

        params:
            name: name of current group
            save: save the result to a csv file or not
            group: current group
            data_dir: directory of data source
            result_dir: directory which will be use to save the data
            logger: the universal logger
            prob: default is one, fraction of data that will be accept by the program
    """
    cerebro = bt.Cerebro()

    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤
    cerebro.addanalyzer(TotalValue, _name="_TotalValue") # 账户总值

    cerebro.addstrategy(MyStrategy, group=group, logger=logger)

    # 将买卖数量固定为100的倍数
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)

    # 将文件夹下每一个CSV数据导入策略模型
    files = os.listdir(data_dir)
    for ind, file in enumerate(files):
        if np.random.rand() > prob:
            continue
        data = GenericCVS_extend(
            dataname=data_dir+'/'+file,
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
    cerebro.addobserver(bt.observers.TimeReturn)

    logger.info('启动资金: %.2f' % cerebro.broker.getvalue())

    result = cerebro.run()
    risk_free_rate = 0.017
    risk_free_rate_daily = (1 + risk_free_rate)**(1/252) - 1

    df = pd.DataFrame({"Account value":result[0].p.account_value})
    df['Account value'] = df['Account value']-500000
    df['Return'] = df['Account value'].pct_change()
    # Annual risk free rate
    risk_free_rate_annual = 0.017  # 1.7%
    # Convert annual risk free rate to daily
    risk_free_rate_daily = (1 + risk_free_rate_annual)**(1/252) - 1
    # Calculate the excess returns by subtracting the daily risk-free rate from the daily returns
    df['Excess Return'] = df['Return'] - risk_free_rate_daily
    # Calculate the Sharpe Ratio
    sharpe_ratio = df['Excess Return'].mean() / df['Excess Return'].std()
    # Annualize the Sharpe Ratio
    sharpe_ratio = sharpe_ratio * np.sqrt(252)
    
    annual_return = []
    for year in result[0].analyzers._AnnualReturn.get_analysis().items():
        annual_return.append(year[1])

    logger.info('期末价值: %.2f' % cerebro.broker.getvalue())
    logger.info(f'Annual Return: {np.mean(annual_return)*2}')
    logger.info(f'Sharp Ratio: {sharpe_ratio}')
    logger.info(f'Max DrawDown: {result[0].analyzers._DrawDown.get_analysis()["max"]["drawdown"]*2}%')

    
    account_value = np.array(result[0].p.account_value)
    account_value = account_value/(account_value[0]/2)-1
    index_mean = result[0].p.index_mean
    index_mean_long = result[0].p.index_mean_long
    if result[0].p.hedge:
        index_mean_short = result[0].p.index_mean_short
    total_trend = np.array(result[0].p.total_trend)
    total_trend = total_trend/total_trend[0]
    
    df = pd.DataFrame(data={'Market trend':total_trend, f"{name} Return":account_value})

    order_long = result[0].p.order_summery_long
    df_long = pd.DataFrame(list(order_long.items()), columns=['Bond name', 'Time ordered']).sort_values(by='Time ordered', ascending=False).head(50)
    if result[0].p.hedge:
        order_short = result[0].p.order_summery_short
        df_short = pd.DataFrame(list(order_short.items()), columns=['Bond name', 'Time ordered']).sort_values(by='Time ordered', ascending=False)
        # df_short.to_csv(os.path.join(result_dir, "Shorted.csv"), index=False)
        df_short = df_short.head(50)
    

    if save and os.path.exists(os.path.join(result_dir, "Result.csv")):
        temp_df = pd.read_csv(os.path.join(result_dir, "Result.csv"))
        temp_df[f"{name} Return"] = df[f"{name} Return"].tail(int(len(account_value)/(group+1))).values
        temp_df.to_csv(os.path.join(result_dir, "Result.csv"), mode='w', index = False)
    elif save:
        df.to_csv(os.path.join(result_dir, "Result.csv"), index=False)
    else:
        cerebro.plot(savefig=True, figfilename=os.path.join(result_dir, 'Backtest result'))

        plt.plot(account_value, label="Account value")
        plt.plot(total_trend, label="Market trend")
        plt.title("Account value compared to market trend")
        plt.legend()
        plt.savefig(os.path.join(result_dir,"Account value compared to market trend.png"))
        plt.show()

        plt.plot(index_mean, label="Market index mean")
        plt.plot(index_mean_long, label="Long index mean")
        if result[0].p.hedge:
            plt.plot(index_mean_short, label="Short index mean")
        plt.title("Index mean of the whole market over time")
        plt.legend()
        plt.savefig(os.path.join(result_dir, "Index mean of the whole market over time.png"))
        plt.show()

        if result[0].p.hedge:
            # create the first subplot
            plt.figure(figsize=(10,12))
            plt.subplot(2, 1, 1) # 2 rows, 1 column, index 1
            plt.bar(df_long['Bond name'], df_long['Time ordered'].astype(int))
            plt.xticks(rotation=45)
            plt.xlabel('Bond name')
            plt.ylabel('Time ordered')
            plt.title('Total long time in backtest')

            # create the second subplot
            plt.subplot(2, 1, 2) # 2 rows, 1 column, index 2
            plt.bar(df_short['Bond name'], df_short['Time ordered'].astype(int))
            plt.xticks(rotation=45)
            plt.xlabel('Bond name')
            plt.ylabel('Time ordered')
            plt.title('Total short time in backtest')

            # adjust the layout so that plots do not overlap
            plt.tight_layout()
            plt.savefig(os.path.join(result_dir, "Total trade time in backtest.png"))
            plt.show()
        else:
            plt.bar(df_long['Bond name'], df_long['Time ordered'].astype(int))
            plt.xticks(rotation=45)
            plt.xlabel('Bond name')
            plt.ylabel('Time long')
            plt.title('Total long time in backtest')
            plt.savefig(os.path.join(result_dir, "Total trade time in backtest.png"))
            plt.show()


if __name__ == '__main__':
    
    data_dir = 'C:/Users/21995/Desktop/量化投资/中金/Data/CB_Data_Flux'
    result_dir = os.path.join("Results", 'ReturnSTD')
    if not os.path.isdir(result_dir):
        os.makedirs(result_dir)
    result_dir = os.path.join(pathlib.Path(__file__).parent.resolve(), result_dir)
    logging.basicConfig(filename=os.path.join(result_dir, 'Backtest.log'), filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)
    backTest(name=f"top {0*10} to {0*10+10}%", save=False, group=0, data_dir=data_dir, result_dir=result_dir, logger=logger, prob=1)
    # for a in range(0,10):
    #     print(f"top {a*10} to {a*10+10}%")
    #     backTest(name=f"top {a*10} to {a*10+10}%", save=True, group=a, data_dir=data_dir, result_dir=result_dir, logger=logger, prob=1)