import datetime
import pandas as pd
from util import convert_date, revert_indian_number_format, format_numbers_to_indian_system

def load_data():
    fpath = r'E:\python\ToBeIgnored.csv'
    df = pd.read_csv(fpath)
    l1 = df['0'].tolist()

    calls_data_file_path = r'E:\python\CompleteScrapedData.csv'
    historic_company_data_file_path = r'E:\python\HistoricDataWithCompanyAgain.csv'
    calls_df = pd.read_csv(calls_data_file_path)
    history_df = pd.read_csv(historic_company_data_file_path)

    # PreProcessing
    calls_df['Date'] = calls_df['Date'].apply(convert_date)
    history_df['Date'] = history_df['Date'].apply(convert_date)

    unique_analysts = calls_df['Analyst'].unique()

    # Create a dictionary to store smaller DataFrames for each analyst
    analyst_dfs = {analyst: calls_df[calls_df['Analyst'] == analyst].reset_index(drop=True) for analyst in unique_analysts}

    # Create a dictionary to store smaller DataFrames for each company
    company_data = {company: df.reset_index(drop=True) for company, df in history_df.groupby('Company')}

    return l1, analyst_dfs, company_data

def process_data(start_date, end_date, dur, analyst_to_be_displayed, l1, analyst_dfs, company_data):
    if dur == "1Y":
        x = datetime.timedelta(days=365)
    elif dur == "6M":
        x = datetime.timedelta(days=182)
    elif dur == "3M":
        x = datetime.timedelta(days=91)

    to_be_processed = []
    calls_to_be_processed = {}

    if analyst_to_be_displayed == 'All':
        to_be_processed = list(analyst_dfs.keys())
    else:
        to_be_processed = [analyst_to_be_displayed]

    final_dict = {}

    for broker in analyst_dfs:
        if broker in to_be_processed:
            if start_date < analyst_dfs[broker]['Date'].iloc[-1]:
                if end_date >= analyst_dfs[broker]['Date'].iloc[-1]:
                    filtered_df = analyst_dfs[broker][
                        (analyst_dfs[broker]['Date'] >= start_date) & (analyst_dfs[broker]['Date'] <= end_date) & (
                            ~analyst_dfs[broker]['Company'].isin(l1))]
                    calls_to_be_processed[broker] = filtered_df
                else:
                    continue
            else:
                filtered_df = analyst_dfs[broker][
                    (analyst_dfs[broker]['Date'] >= start_date) & (analyst_dfs[broker]['Date'] <= end_date) & (
                        ~analyst_dfs[broker]['Company'].isin(l1))]
                calls_to_be_processed[broker] = filtered_df

    for broker in calls_to_be_processed:
        bdf = calls_to_be_processed[broker]
        calls = 0
        successes = 0
        percentage=0
        for tar, adv, dat, com in zip(bdf['Target'], bdf['Advice'], bdf['Date'], bdf['Company']):

            call_date = dat
            till_date = call_date + x
            if company_data[com]['Date'].iloc[-1] < till_date:
                continue
            calls += 1

            if adv in ['Buy', 'Neutral', 'Hold', 'Accumulate']:
                reach = company_data[com]['High'][
                    (company_data[com]['Date'] >= call_date) & (company_data[com]['Date'] <= till_date)].max()
            else:
                reach = company_data[com]['Low'][
                    (company_data[com]['Date'] >= call_date) & (company_data[com]['Date'] <= till_date)].min()

            if reach is not None and not pd.isna(reach) and not pd.isna(tar):
                if adv in ['Buy', 'Neutral', 'Hold', 'Accumulate']:
                    if reach >= tar:
                        successes += 1
                else:
                    if reach <= tar:
                        successes += 1

            percentage = (successes / calls) * 100 if calls != 0 else 0
            percentage = round(percentage, 1)
        final_dict[broker] = {"Total Calls in Period: ": calls, "Total Successes in the period: ": successes,
                              "Success %": percentage}

    final_df = pd.DataFrame.from_dict(final_dict, orient='index')
    if len(final_df) > 1:
        final_df = final_df.sort_values(by='Success %', ascending=False)
    return final_df

def sort_data_frame(final_df, sort_by):
    final_df = revert_indian_number_format(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    final_df = final_df.sort_values(by=sort_by, ascending=False)
    final_df = format_numbers_to_indian_system(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    return final_df
