import datetime
import pandas as pd
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


    df = pd.read_csv(fpath)

    #List containing companies whose data/ticker symbol is not correct
    l1 = df['0'].tolist() 

    #path to the Calls data CSV
    calls_data_file_path = r'E:\python\CompleteScrapedData.csv' 

    #Historic stocks data CSV file path
    historic_company_data_file_path = r'E:\python\HistoricDataWithCompanyAgain.csv'
    calls_df = pd.read_csv(calls_data_file_path)
    history_df = pd.read_csv(historic_company_data_file_path)


    # Converting the Date columns to DateTime objects
    calls_df['Date'] = calls_df['Date'].apply(convert_date)
    history_df['Date'] = history_df['Date'].apply(convert_date)

    unique_analysts = calls_df['Analyst'].unique()

    # Create a dictionary to store DataFrames for each analyst
    analyst_dfs = {analyst: calls_df[calls_df['Analyst'] == analyst].reset_index(drop=True) for analyst in unique_analysts}

    # Create a dictionary to store DataFrames for each company
    company_data = {company: df.reset_index(drop=True) for company, df in history_df.groupby('Company')}

    return l1, analyst_dfs, company_data

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


    if analyst_to_be_displayed == 'All':
        to_be_processed = list(analyst_dfs.keys())
    else:
        to_be_processed = [analyst_to_be_displayed]

    #dictionary to be converted to df finally
    final_dict = {}

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
                            ~analyst_dfs[broker]['Company'].isin(l1))]
                    
                    calls_to_be_processed[broker] = filtered_df
                #if end date is also less than the first call
                else:
                    continue
            else:

                # filtered_df consisting of all the calls made between the dates and with the company not in l1
                filtered_df = analyst_dfs[broker][
                    (analyst_dfs[broker]['Date'] >= start_date) & (analyst_dfs[broker]['Date'] <= end_date) & (
                        ~analyst_dfs[broker]['Company'].isin(l1))]
                calls_to_be_processed[broker] = filtered_df
    # the calls_to_be_processed dictionary to later be used to showcase on the frontend all the calls considered while doing the analysis of a given broker 


    for broker in calls_to_be_processed:
        bdf = calls_to_be_processed[broker]
        calls = 0
        successes = 0
        percentage=0 
        for tar, adv, dat, com in zip(bdf['Target'], bdf['Advice'], bdf['Date'], bdf['Company']):

            call_date = dat
            till_date = call_date + x #x created from dur at the beginning

            #if latest data is older than the till date, continue
            if company_data[com]['Date'].iloc[-1] < till_date:
                continue
            calls += 1


            # need to do this using reco price asap
            # if all these them reach using high else using low
            if adv in ['Buy', 'Neutral', 'Hold', 'Accumulate']:
                #defining reach as the highest point the company reached in the given period to be compared with the target to deem success
                reach = company_data[com]['High'][
                    (company_data[com]['Date'] >= call_date) & (company_data[com]['Date'] <= till_date)].max()
            else:

                #defining reach as the lowest point the company reached in the given period to be compared with the target to deem success
                reach = company_data[com]['Low'][
                    (company_data[com]['Date'] >= call_date) & (company_data[com]['Date'] <= till_date)].min()

            # isna checks if the value is NaN
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

    # final_df that is used to render the df
    final_df = pd.DataFrame.from_dict(final_dict, orient='index')
    if len(final_df) > 1:
        final_df = final_df.sort_values(by='Success %', ascending=False)
    if final_df is not None:
        format_numbers_to_indian_system(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    return final_df

def sort_data_frame(final_df, sort_by):
    final_df = revert_indian_number_format(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    final_df = final_df.sort_values(by=sort_by, ascending=False)
    final_df = format_numbers_to_indian_system(final_df, ["Total Calls in Period: ", "Total Successes in the period: "])
    return final_df
