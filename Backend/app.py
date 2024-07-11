from flask import Flask, render_template, request, session, redirect, url_for, jsonify, redirect
from main import load_data, process_data, sort_data_frame, hot_stocks_backend,recommended_stocks,rankgen
from util import convert_date, format_numbers_to_indian_system
import pandas as pd
import datetime
import plotly.graph_objs as go
import urllib.parse

app = Flask(__name__)
app.secret_key = 'koinahibtayega'  # Needed to encrypt session data
# Global definition of l1, analysts, and company data to ensure they are loaded only once, saving time
analyst_rank={}
# Global definition of final_df to make sorting easier as it won't have to be processed again every time sorting has to be done
columns = ['Total Calls in Period: ', 'Total Successes in the period: ', 'Success %']
final_df = pd.DataFrame(columns=columns)
recommendation_df=pd.DataFrame(columns=columns)
rank_df=pd.DataFrame(columns=columns)
stocks_df=pd.DataFrame(columns=columns)
form_values_rec={}
unique_company={}
calls_to_be_processed= {}
rec_all_calls={}
calls_by_company_stocks={}
company_list,l1, analyst_dfs, company_data,list_of_unique_analysts, calls_by_company, calls_df = load_data()
dropdown_options = {
'period': ['1Y', '6M', '3M'],
'analyst': list_of_unique_analysts
    }
calls_df=pd.DataFrame(columns=columns)
# Default values for the forms
date_to_be_considered =datetime.date.today()-datetime.timedelta(days=365)
dropdown_options_portfolio_gen={
    'Company':company_list,
}
default_form_values = {
    'start-date': '2018-01-01',
    'end-date': str(date_to_be_considered),
    'period': '1Y',
    'analyst': 'All'
}
default_form_values_stock={
    'start-date': '2018-01-01',
    'end-date': str(date_to_be_considered+datetime.timedelta(days=365)),
}
default_form_values_rec={
    'priority': 'Number of Recommendations',
    'period':'30D',
    'num':'All',
    'sort-by':'Final Factor',
    'rank-consider':'yes',
    'start-date': '2021-01-01',
    'end-date': str(date_to_be_considered),
    'period-considered': '1Y',
    'upside-factor-weight':'50%',
    'minimum-upside-current':'0%',
    'market-cap':'All'
}
default_form_values_ranker={
    'start-date': '2021-01-01',
    'end-date': str(date_to_be_considered),
    'period-considered': '1Y'
}
# Dropdown options for analyst and recommendations

dropdown_options_for_rec={
    'priority':['Number of Recommendations','Average Upside','Average Target','Max Upside','Max Target'],
    'period':['1D','5D','7D','15D','30D','120D'],
    'num':['5','10','20','30','All'],
    'sort-by':['Number of Recommendations','Average Upside','Average Target','Max Upside','Max Target','Norm Wt Num Calls','Norm Wt Avg Upside Curr','Final Factor','Max Upside Current','Average Upside Current'],
    'period-considered': ['1Y', '6M', '3M'],
    'weighted-options':['Weighted Target','Weighted Upside','Weighted Upside Current','Weighted Number of Calls'],
    'upside-factor-weight':['100%','90%','80%','70%','60%','50%','40%','30%','20%','10%','0%'],
    'minimum-upside-current':['0%','10%','15%','20%'],
    'market-cap':['0-500','500-2k','2k-5k','5k-20k','20k+','All']
}
#Home page route 
@app.route('/')
def index():
    global l1, analyst_dfs, company_data,list_of_unique_analysts, calls_by_company, calls_df, dropdown_options
    session.clear()  
    global default_form_values
    global final_df
    date_to_be_considered =datetime.date.today()-datetime.timedelta(days=365)
    default_form_values = {
    'start-date': '2018-01-01',
    'end-date': str(date_to_be_considered),
    'period': '1Y',
    'analyst': 'All'
    }
    l1, analyst_dfs, company_data,list_of_unique_analysts, calls_by_company, calls_df = load_data()
    dropdown_options = {
    'period': ['1Y', '6M', '3M'],
    'analyst': list_of_unique_analysts
    }
    session['form_values'] = default_form_values
    return render_template('index.html')

#Analyst view
@app.route('/analyst',methods=['GET', 'POST'])
def analyst():
    
    if 'form_values' not in session:
        session[form_values]= default_form_values
    
    global calls_to_be_processed
    global final_df
    global unique_company
    if request.method == 'POST':
        form_values = {
            'start-date': request.form.get('start-date', default_form_values['start-date']),
            'end-date': request.form.get('end-date', default_form_values['end-date']),
            'period': request.form.get('period', default_form_values['period']),
            'analyst': request.form.get('analyst', default_form_values['analyst'])
        }
        session['form_values'] = form_values
    else:
        form_values = session['form_values']

    return render_template('analyst.html',df=final_df, form_values=form_values, dropdown_options=dropdown_options)

@app.route('/generate_data', methods=['POST'])
def generate_data():
    global calls_to_be_processed
    global final_df
    global unique_company
    form_values = {
        'start-date': request.form['start-date'],
        'end-date': request.form['end-date'],
        'period': request.form['period'],
        'analyst': request.form['analyst']
    }
    session['form_values'] = form_values

    start_date = convert_date(form_values['start-date'])
    end_date = convert_date(form_values['end-date'])
    dur = form_values['period']
    analyst_to_be_displayed = form_values['analyst']

    final_df,calls_to_be_processed,unique_company = process_data(start_date, end_date, dur, analyst_to_be_displayed, l1, analyst_dfs, company_data)
    return render_template('analyst.html', df=final_df, form_values=form_values, dropdown_options=dropdown_options)

@app.route('/sort_table', methods=['POST'])
def sort_table():
    global final_df
    if request.method == 'POST':
        sort_by = request.form['sort_by']
        if final_df is not None and len(final_df) > 1:
            final_df = sort_data_frame(final_df, sort_by)
        return render_template('analyst.html', df=final_df, form_values=session['form_values'], dropdown_options=dropdown_options)

# To return analyst wise calls to modal
@app.route('/get_analyst_details')
def get_analyst_details():
    analyst = request.args.get('analyst')
    analyst = urllib.parse.unquote(analyst)
    if analyst in calls_to_be_processed:
        details_df = calls_to_be_processed[analyst].copy()
        if 'Remarks(if any)' in details_df.columns:
            details_df.drop(['Remarks(if any)'], axis=1,inplace=True)
        if 'To Be Taken'in details_df.columns:
            details_df.drop(['To Be Taken'], axis=1,inplace=True)
        details_df['Market Cap']=pd.to_numeric(details_df['Market Cap'],errors='coerce')
        details_df['Market Cap']=details_df['Market Cap']/(10**7)
        details_df['Target']=details_df['Target'].round(1)
        details_df=format_numbers_to_indian_system(details_df,['Market Cap'])
        details_df.reset_index(drop=True, inplace=True)
        details_html = details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this analyst.'})

#To return analyst wise company summaries
@app.route('/get_analyst_company_details')
def get_analyst_company_details():
    analyst = request.args.get('analyst')
    analyst = urllib.parse.unquote(analyst)
    if analyst in unique_company:
        details_df=unique_company[analyst]
        details_html=details_df.to_html(classes='inlay-table')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this analyst.'})

#To stocks.html
@app.route('/stocks')
def stocks():
    global default_form_values_stock
    global stocks_df
    return render_template('stocks.html',df=stocks_df,form_values =default_form_values_stock)
@app.route('/generate_stocks_info',methods=['POST'])
def generate_stocks_info():
    global stocks_df
    global calls_by_company_stocks
    form_values = {
        'start-date': request.form['start-date'],
        'end-date': request.form['end-date']
    }
    

    start_date = convert_date(form_values['start-date'])
    end_date = convert_date(form_values['end-date'])
    stocks_df,calls_by_company_stocks=hot_stocks_backend(start_date,end_date,calls_by_company,l1)
    return render_template('stocks.html',df=stocks_df,form_values=form_values,)
    
@app.route('/get_stocks_details')
def get_stocks_details():
    global calls_by_company_stocks
    company = request.args.get('company')
    company = urllib.parse.unquote(company)
    if company in calls_by_company_stocks:
        details_df=calls_by_company_stocks[company].copy()
        if 'Remarks(if any)' in details_df.columns:
            details_df.drop(['Remarks(if any)'], axis=1,inplace=True)
        if 'To Be Taken'in details_df.columns:
            details_df.drop(['To Be Taken'], axis=1,inplace=True)
        details_df['Market Cap']=pd.to_numeric(details_df['Market Cap'],errors='coerce')
        details_df['Market Cap']=details_df['Market Cap']/(10**7)
        details_df=format_numbers_to_indian_system(details_df,['Market Cap'])
        details_df=details_df.reset_index(drop=True)
        details_html=details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this company.'})

#To recommendation.html
@app.route('/recommendation')
def recommendation():
    global recommendation_df
    # global rec_all_calls
    # global dropdown_options_for_rec
    # global default_form_values_rec
    # global analyst_rank
    # global analyst_dfs
    # global company_data
    # priority=default_form_values_rec['priority']
    # period=default_form_values_rec['period']
    # num = default_form_values_rec['num']
    # sort_by = default_form_values_rec['sort-by']
    # rank_consider = default_form_values_rec['rank-consider']
    # start_date=default_form_values_rec['start-date']
    # end_date= default_form_values_rec['end-date']
    # dur=default_form_values_rec['period-considered']
    # wtcon=True if rank_consider=="yes" else False
    # df,rec_all_calls=recommended_stocks(start_date, end_date, dur, analyst_dfs, company_data,rank_consider,sort_by,priority,period,num,calls_df,l1,analyst_rank)
    # columns = ['Total Calls in Period: ', 'Total Successes in the period: ', 'Success %']
    # df = pd.DataFrame(columns=columns)
    wtcon=True
    return render_template('recommendation.html',df=recommendation_df, dropdown_options_for_rec=dropdown_options_for_rec,form_values=default_form_values_rec,wtcon=wtcon)
@app.route('/generate_rec',methods=['POST'])
def generate_rec():
    global rec_all_calls
    global dropdown_options_for_rec
    global default_form_values_rec
    global analyst_rank
    global analyst_dfs
    global company_data
    global recommendation_df
    global form_values_rec
    form_values_rec={

        #'priority':request.form['priority'],
        'period':request.form['period'],
        'num':request.form['num'],
        'sort-by': request.form['sort-by'],
        'rank-consider':request.form.get('rank-consider','no'),
        'start-date':request.form['start-date'],
        'end-date':request.form['end-date'],
        'period-considered':request.form['period-considered'],
        'upside-factor-weight':request.form['upside-factor-weight'],
        'minimum-upside-current':request.form['minimum-upside-current'],
        'market-cap':request.form['market-cap']
    }
    #priority=form_values_rec['priority']
    priority='Number of Recommendations'
    sort_by=form_values_rec['sort-by']
    period=form_values_rec['period']
    num = form_values_rec['num']
    rank_consider=form_values_rec['rank-consider']
    start_date=convert_date(form_values_rec['start-date'])
    end_date= convert_date(form_values_rec['end-date'])
    dur=form_values_rec['period-considered']
    upside_factor_weight=form_values_rec['upside-factor-weight']
    upside_filter=form_values_rec['minimum-upside-current']
    wtcon=True if rank_consider=="yes" else False
    mcap=form_values_rec['market-cap']
    recommendation_df,rec_all_calls=recommended_stocks(mcap,upside_filter,upside_factor_weight,start_date, end_date, dur, analyst_dfs, company_data,rank_consider,sort_by,priority,period,num,calls_df,l1,analyst_rank)
    if num =='All':
        return render_template('recommendation.html',df=recommendation_df, dropdown_options_for_rec=dropdown_options_for_rec,form_values=form_values_rec,wtcon=wtcon)
    else:
        temp_df=recommendation_df.head(int(num))
        return render_template('recommendation.html',df=temp_df, dropdown_options_for_rec=dropdown_options_for_rec,form_values=form_values_rec,wtcon=wtcon)

 

@app.route('/get_stocks_details_for_rec')
def get_stocks_details_for_rec():
    global rec_all_calls
    company = request.args.get('company')
    company = urllib.parse.unquote(company)
    if company in rec_all_calls:
        details_df=rec_all_calls[company].copy()
        if 'Remarks(if any)' in details_df.columns:
            details_df.drop(['Remarks(if any)'], axis=1,inplace=True)
        if 'To Be Taken'in details_df.columns:
            details_df.drop(['To Be Taken'], axis=1,inplace=True)
        if 'Market Cap' in details_df.columns:
            details_df.drop(['Market Cap'], axis=1,inplace=True)
        
        # details_df['Market Cap']=pd.to_numeric(details_df['Market Cap'],errors='coerce')
        # details_df['Market Cap']=details_df['Market Cap']/(10**7)
        # details_df=format_numbers_to_indian_system(details_df,['Market Cap'])
        details_html=details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this company.'})

@app.route('/generate_stock_graph')
def generate_stock_graph():
    company = request.args.get('company')
    company = urllib.parse.unquote(company)
    global rec_all_call
    #print(rec_all_calls)
    if company in rec_all_calls:
        df = rec_all_calls[company].copy()
        hdf = company_data[company].copy()
        
        first_call_date = df['Date'].min()
        today = datetime.date.today()
        to_be_plotted = hdf[(hdf['Date'] >= first_call_date) & (hdf['Date'] <= today)]
        date_list = to_be_plotted['Date'].tolist()
        close_list = to_be_plotted['Close'].tolist()
        
        trace = go.Scatter(x=date_list, y=close_list, mode='lines', name=f'Price for {company}')
        
        trace_horizontal_lines = []
        trace_markers = []
        analyst_colors = ['orange', 'green', 'red', 'black','purple']  
        for index, row in df.iterrows():
            color_index = index % len(analyst_colors)
            analyst_color = analyst_colors[color_index]
            trace_line = go.Scatter(x=date_list, y=[row['Target']] * len(date_list), mode='lines', 
                                    line=dict(color=analyst_color, dash='dash'), name=row['Analyst'])
            trace_horizontal_lines.append(trace_line)
            
            trace_marker = go.Scatter(x=[row['Date']], y=[row['Target']], mode='markers', 
                                      marker=dict(symbol='circle', size=10, color=analyst_color),
                                      name=f'Call by {row["Analyst"]} - {row["Upside"]}%')
            trace_markers.append(trace_marker)
        
        fig = go.Figure(data=[trace] + trace_horizontal_lines + trace_markers)
        fig.update_layout(title=f'Stock Prices for {company}', xaxis=dict(title='Date'), yaxis=dict(title='Price'))

        # Convert figure to JSON to send to frontend
        graph_json = fig.to_json()
        
        return jsonify({'graph': graph_json})
    
    return jsonify({'graph': ''})

@app.route('/show_full_table',methods=['POST'])
def show_full_rec_table():
    global rec_all_calls
    global dropdown_options_for_rec
    global default_form_values_rec
    global analyst_rank
    global analyst_dfs
    global company_data
    global recommendation_df
    global form_values_rec

    rank_consider=form_values_rec['rank-consider']

    wtcon=True if rank_consider=="yes" else False
    return render_template('recommendation.html',df=recommendation_df, dropdown_options_for_rec=dropdown_options_for_rec,form_values=form_values_rec,wtcon=wtcon)
@app.route('/ranker')
def ranker():
    global rank_df
    return render_template('ranker.html',df=rank_df,dropdown_options_for_rec=dropdown_options_for_rec,form_values=default_form_values_ranker)

@app.route('/generate_rank',methods=['POST'])
def generate_rank():
    global analyst_rank
    global analyst_dfs
    global company_data
    global rank_df
    form_values_rank={
        'start-date':request.form['start-date'],
        'end-date':request.form['end-date'],
        'period-considered':request.form['period-considered']
    }
    start_date=convert_date(form_values_rank['start-date'])
    end_date= convert_date(form_values_rank['end-date'])
    dur=form_values_rank['period-considered']
    dict1,rank_df,dict_df=rankgen(start_date, end_date, dur, analyst_dfs, company_data, l1,analyst_rank)
    df= pd.DataFrame(list(dict1.items()),columns=['Analyst','Score'])
    return render_template('ranker.html',df=rank_df,dropdown_options_for_rec=dropdown_options_for_rec,form_values=form_values_rank)

#reset session
@app.route('/reset_session')
def reset_session():
    session['form_values'] = default_form_values
    return redirect(url_for('index'))

if __name__ == "__main__":
    
    app.run(debug=True)
