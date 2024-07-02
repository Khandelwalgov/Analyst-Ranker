from flask import Flask, render_template, request, session, redirect, url_for, jsonify, redirect
from main import load_data, process_data, sort_data_frame, hot_stocks_backend,recommended_stocks
from util import convert_date
import pandas as pd

app = Flask(__name__)
app.secret_key = 'koinahibtayega'  # Needed to encrypt session data

# Global definition of l1, analysts, and company data to ensure they are loaded only once, saving time
l1, analyst_dfs, company_data,list_of_unique_analysts, calls_by_company, calls_df = load_data()
analyst_rank={}
# Global definition of final_df to make sorting easier as it won't have to be processed again every time sorting has to be done
columns = ['Total Calls in Period: ', 'Total Successes in the period: ', 'Success %']
final_df = pd.DataFrame(columns=columns)
unique_company={}
calls_to_be_processed= {}
rec_all_calls={}

# Default values for the forms
default_form_values = {
    'start-date': '2000-01-01',
    'end-date': '2023-06-13',
    'period': '1Y',
    'analyst': 'All'
}
default_form_values_rec={
    'priority': 'Number of Recommendations',
    'period':'30D',
    'num':'10',
    'sort-by':'Average Upside'
}

# Dropdown options for analyst and recommendations
dropdown_options = {
    'period': ['1Y', '6M', '3M'],
    'analyst': list_of_unique_analysts
}
dropdown_options_for_rec={
    'priority':['Number of Recommendations','Average Upside','Average Target','Max Upside','Max Target'],
    'period':['1D','5D','7D','15D','30D','120D'],
    'num':['5','10','15'],
    'sort-by':['Number of Recommendations','Average Upside','Average Target','Max Upside','Max Target']
}

#Home page route 
@app.route('/')
def index():
    session.clear()  
    global default_form_values
    global final_df
    default_form_values = {
    'start-date': '2000-01-01',
    'end-date': '2023-06-13',
    'period': '1Y',
    'analyst': 'All'
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
        return render_template('index.html', df=final_df, form_values=session['form_values'], dropdown_options=dropdown_options)

# To return analyst wise calls to modal
@app.route('/get_analyst_details')
def get_analyst_details():
    analyst = request.args.get('analyst')
    if analyst in calls_to_be_processed:
        details_df = calls_to_be_processed[analyst]
        details_html = details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this analyst.'})

#To return analyst wise company summaries
@app.route('/get_analyst_company_details')
def get_analyst_company_details():
    analyst = request.args.get('analyst')
    if analyst in unique_company:
        details_df=unique_company[analyst]
        details_html=details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this analyst.'})

#To stocks.html
@app.route('/stocks')
def stocks():
    df=hot_stocks_backend(calls_by_company,l1)
    return render_template('stocks.html',df=df)

@app.route('/get_stocks_details')
def get_stocks_details():
    company = request.args.get('company')
    if company in calls_by_company:
        details_df=calls_by_company[company]
        details_html=details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this company.'})

#To recommendation.html
@app.route('/recommendation')
def recommendation():
    global rec_all_calls
    global dropdown_options_for_rec
    global default_form_values_rec
    priority=default_form_values_rec['priority']
    period=default_form_values_rec['period']
    num = default_form_values_rec['num']
    sort_by = default_form_values_rec['sort-by']
    df,rec_all_calls=recommended_stocks(sort_by,priority,period,num,calls_df,l1,analyst_rank)
    return render_template('recommendation.html',df=df, dropdown_options_for_rec=dropdown_options_for_rec,form_values=default_form_values_rec)
@app.route('/generate_rec',methods=['POST'])
def generate_rec():
    global rec_all_calls
    global dropdown_options_for_rec
    global default_form_values_rec
    form_values_rec={

        'priority':request.form['priority'],
        'period':request.form['period'],
        'num':request.form['num'],
        'sort-by': request.form['sort-by']
    }
    priority=form_values_rec['priority']
    sort_by=form_values_rec['sort-by']
    period=form_values_rec['period']
    num = form_values_rec['num']
    df,rec_all_calls=recommended_stocks(sort_by,priority,period,num,calls_df,l1,analyst_rank)
    return render_template('recommendation.html',df=df, dropdown_options_for_rec=dropdown_options_for_rec,form_values=form_values_rec)
 

@app.route('/get_stocks_details_for_rec')
def get_stocks_details_for_rec():
    global rec_all_calls
    company = request.args.get('company')
    if company in rec_all_calls:
        details_df=rec_all_calls[company]
        details_html=details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this company.'})

#reset session
@app.route('/reset_session')
def reset_session():
    session['form_values'] = default_form_values
    return redirect(url_for('index'))

if __name__ == "__main__":
    
    app.run(debug=True)
