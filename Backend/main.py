import datetime
import pandas as pd
import numpy as np
from util import convert_date, revert_indian_number_format, format_numbers_to_indian_system
'''
main.py file with the functions to be used with different paths and triggers in app.py

load_data: a function to be used ideally before the site loads, to load the CSVs, make the "blacklist" i.e. the list with companies whose tickers arent available. Also makes the Date column to DateTime objects, returns list and company and calls df

process_data: process the data completely, and gives the required df as return

sort_data_frame: sorts the present dataframe with the passed value, returns the sorted df
'''
def load_data():

    #Path to the "ToBeIgnored" CSV file
    fpath = r'E:\python\ToBeIgnored.csv' 
    #Path to list of unique analysts
    fpathanalysts=r'E:\python\UniqueAnalysts1.csv'
    #path to the Calls data CSV
    # old file calls_data_file_path = r'E:\python\UpdatedCallsWithRecoPriceAndTickers.csv' 
    calls_data_file_path = r'E:\python\CallsWithUpdatedUpside.csv'
    #Historic stocks data CSV file path
    # old file historic_company_data_file_path = r'E:\python\HistoricDataWithCompanyAgain.csv'
    historic_company_data_file_path = r'E:\python\HistoricDataUpdatedTickersCorrectly.csv'

    df_for_analysts=pd.read_csv(fpathanalysts)
    df = pd.read_csv(fpath)
    calls_df = pd.read_csv(calls_data_file_path)
    history_df = pd.read_csv(historic_company_data_file_path)


    list_of_unique_analysts=df_for_analysts['0']
    #List containing companies whose data/ticker symbol is not correct
    l1 = df['0'].tolist() 

    # Converting the Date columns to DateTime objects
    calls_df['Date'] = calls_df['Date'].apply(convert_date)
    history_df['Date'] = history_df['Date'].apply(convert_date)

    unique_analysts = calls_df['Analyst'].unique()

    # Create a dictionary to store DataFrames for each analyst
    analyst_dfs = {analyst: calls_df[calls_df['Analyst'] == analyst].reset_index(drop=True) for analyst in unique_analysts}

    # Create a dictionary to store DataFrames for each company
    company_data = {company: df.reset_index(drop=True) for company, df in history_df.groupby('Company')}

    calls_by_company= {company: df.reset_index(drop=True).sort_values(by="Date",ascending=False) for company, df in calls_df.groupby('Company')}


    return l1, analyst_dfs, company_data,list_of_unique_analysts, calls_by_company

def process_data(start_date, end_date, dur, analyst_to_be_displayed, l1, analyst_dfs, company_data):

    # Use dur to set corresponding 'x' i.e. periods to focus on for success by using timedelta
    if dur == "1Y":
        x = datetime.timedelta(days=365)
    elif dur == "6M":
        x = datetime.timedelta(days=182)
    elif dur == "3M":
        x = datetime.timedelta(days=91)

    # List of the analysts who have to be processed
    to_be_processed = []

    #Dictionary for the calls for each analyst to be processed in the form dict[Analyst]= Dataframe segment to be processed
    calls_to_be_processed = {}
    calls_after=datetime.date(2018,1,1)

    if analyst_to_be_displayed == 'All':
        to_be_processed = list(analyst_dfs.keys())
    else:
        to_be_processed = [analyst_to_be_displayed]

    #dictionary to be converted to df finally
    final_dict = {}
    unique_company={}
    # Traverses all analysts available
    for broker in analyst_dfs:

        #All analysts in the list
        if broker in to_be_processed:
            
            # if the start date is less than the first call made by the broker and end date is greater than that
            # Need to put a warning message that pops up to warn the user but not hinder the process
            if start_date < analyst_dfs[broker]['Date'].iloc[-1]:
                if end_date >= analyst_dfs[broker]['Date'].iloc[-1]:

                    # filtered_df consisting of all the calls made between the dates and with the company not in l1
                    filtered_df = analyst_dfs[broker][
                        (analyst_dfs[broker]['Date'] >= start_date) & (analyst_dfs[broker]['Date'] <= end_date) & (
                            ~analyst_dfs[broker]['Company'].isin(l1)) & (analyst_dfs[broker]['Date'] >= calls_after)&(analyst_dfs[broker]['To Be Taken']==1)]
                    
                    calls_to_be_processed[broker] = filtered_df.reset_index(drop=True)
                #if end date is also less than the first call
                else:
                    continue
            else:

                # filtered_df consisting of all the calls made between the dates and with the company not in l1
                filtered_df = analyst_dfs[broker][
                    (analyst_dfs[broker]['Date'] >= start_date) & (analyst_dfs[broker]['Date'] <= end_date) & (
                        ~analyst_dfs[broker]['Company'].isin(l1)) & (analyst_dfs[broker]['Date'] >= calls_after) &(analyst_dfs[broker]['To Be Taken']==1)]
                calls_to_be_processed[broker] = filtered_df
    # the calls_to_be_processed dictionary to later be used to showcase on the frontend all the calls considered while doing the analysis of a given broker 


    for broker in calls_to_be_processed:
        bdf = calls_to_be_processed[broker]
        sum_for_average=datetime.timedelta(days=0)
        calls = 0
        successes = 0
        percentage=0 
        broker_company={}
        for tar, adv, dat, com, reco in zip(bdf['Target'], bdf['Advice'], bdf['Date'], bdf['Company'],bdf["Reco"]):
            tar = float(tar)
            reco = float(reco)
            call_date = dat
            till_date = call_date + x #x created from dur at the beginning
            cdf=company_data[com]
            reach =0
            #if latest data is older than the till date, continue
            if cdf['Date'].iloc[-1] < till_date:
                continue
            calls += 1
            if com not in broker_company:
                broker_company[com]=1
            else:
                broker_company[com]+=1

            # need to do this using reco price asap
            # if all these them reach using high else using low
            if reco !=None:
                if reco<tar:
                    reach = cdf['High'][
                    (cdf['Date'] >= call_date) & (cdf['Date'] <= till_date)].max()
                else:
                    reach = cdf['Low'][
                    (cdf['Date'] >= call_date) & (cdf['Date'] <= till_date)].min()
            else:
                if adv in ['Buy', 'Neutral', 'Hold', 'Accumulate']:
                    #defining reach as the highest point the company reached in the given period to be compared with the target to deem success
                    reach = cdf['High'][
                        (cdf['Date'] >= call_date) & (cdf['Date'] <= till_date)].max()
                else:

                    #defining reach as the lowest point the company reached in the given period to be compared with the target to deem success
                    reach = cdf['Low'][
                        (cdf['Date'] >= call_date) & (cdf['Date'] <= till_date)].min()

            # isna checks if the value is NaN
            if reach is not None and not pd.isna(reach) and not pd.isna(tar):
                if reco is not None:
                    if (reco < tar and reach >= tar) or (reco > tar and reach <= tar):
                        successes += 1

                else:
                    if (adv in ['Buy', 'Neutral', 'Hold', 'Accumulate'] and reach >= tar) or (adv not in ['Buy', 'Neutral', 'Hold', 'Accumulate'] and reach <= tar):
                        successes += 1
                
                if reco is not None:

                    if (reco < tar and reach >= tar):  
                        cdf_dates = cdf["Date"].to_numpy()
                        # Find the index where the call_date would fit in the sorted array
                        start_index = np.searchsorted(cdf_dates, call_date)

                        # Make sure that start_index is within the bounds of the dataframe
                        start_index = min(start_index, len(cdf) - 1)
                        for d,h in zip(cdf["Date"].iloc[start_index:], cdf["High"].iloc[start_index:]):
                            if h>=tar:
                                sum_for_average+=(d-call_date)
                                break
                    elif (reco > tar and reach <= tar):
                        cdf_dates = cdf["Date"].to_numpy()
                        # Find the index where the call_date would fit in the sorted array
                        start_index = np.searchsorted(cdf_dates, call_date)

                        # Make sure that start_index is within the bounds of the dataframe
                        start_index = min(start_index, len(cdf) - 1)
                        for d,l in zip(cdf["Date"].iloc[start_index:], cdf["Low"].iloc[start_index:]):
                            if l<=tar:
                                sum_for_average+=(d-call_date)
                                break
                else:
                    if (adv in ['Buy', 'Neutral', 'Hold', 'Accumulate'] and reach >= tar):  
                        cdf_dates = cdf["Date"].to_numpy()
                        # Find the index where the call_date would fit in the sorted array
                        start_index = np.searchsorted(cdf_dates, call_date)

                        # Make sure that start_index is within the bounds of the dataframe
                        start_index = min(start_index, len(cdf) - 1)
                        for d,h in zip(cdf["Date"].iloc[start_index:], cdf["High"].iloc[start_index:]):
                            if h>=tar:
                                sum_for_average+=(d-call_date)
                                break
                    elif (adv not in ['Buy', 'Neutral', 'Hold', 'Accumulate'] and reach <= tar):
                        cdf_dates = cdf["Date"].to_numpy()
                        # Find the index where the call_date would fit in the sorted array
                        start_index = np.searchsorted(cdf_dates, call_date)

                        # Make sure that start_index is within the bounds of the dataframe
                        start_index = min(start_index, len(cdf) - 1)
                        
                        for d,l in zip(cdf["Date"].iloc[start_index:], cdf["Low"].iloc[start_index:]):
                            if l<=tar:
                                sum_for_average+=(d-call_date)
                                break
                
        unique_stocks=len(broker_company)
        percentage = (successes / calls) * 100 if calls != 0 else 0
        percentage = round(percentage, 1)
        avg_time = (sum_for_average/calls).days if calls!=0 else 0
        final_dict[broker] = {"Total Calls in Period: ": calls, "Total Successes in the period: ": successes,
                              "Success %": percentage, "Average days taken by successful calls" : avg_time,"No. of Unique Stocks":unique_stocks}
        unique_company[broker]=pd.DataFrame([broker_company], index=[broker]).transpose().sort_values(by=broker,ascending=False)

    # final_df that is used to render the df
    final_df = pd.DataFrame.from_dict(final_dict, orient='index')
    if len(final_df) > 1:
        final_df = final_df.sort_values(by='Success %', ascending=False)
    if final_df is not None:
        format_numbers_to_indian_system(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    return final_df,calls_to_be_processed, unique_company

def sort_data_frame(final_df, sort_by):
    final_df = revert_indian_number_format(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    final_df = final_df.sort_values(by=sort_by, ascending=False)
    final_df = format_numbers_to_indian_system(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    return final_df

def hot_stocks_backend(calls_by_company, l1):
    companies = []
    number_of_calls = []
    
    for company in calls_by_company:
        if company not in l1:
            companies.append(company)
            number_of_calls.append(len(calls_by_company[company]))
    
    # Create a DataFrame from dictionaries or tuples where each tuple is a separate row
    data = {'Company': companies, 'Number of calls made': number_of_calls}
    stocks_details_df = pd.DataFrame(data)
    
    # Sort DataFrame by 'Number of calls made' in descending order
    stocks_details_df = stocks_details_df.sort_values(by="Number of calls made", ascending=False)
    
    return stocks_details_df
