from flask import Flask, render_template, request, session, redirect, url_for, jsonify
from main import load_data, process_data, sort_data_frame
from util import convert_date
import pandas as pd

app = Flask(__name__)
app.secret_key = 'koinahibtayega'  # Needed to encrypt session data

# Global definition of l1, analysts, and company data to ensure they are loaded only once, saving time
l1, analyst_dfs, company_data,list_of_unique_analysts = load_data()

# Global definition of final_df to make sorting easier as it won't have to be processed again every time sorting has to be done
columns = ['Total Calls in Period: ', 'Total Successes in the period: ', 'Success %']
final_df = pd.DataFrame(columns=columns)
calls_to_be_processed= {}
# Default values for the form
default_form_values = {
    'start-date': '2000-01-01',
    'end-date': '2023-06-13',
    'period': '1Y',
    'analyst': 'All'
}

# Dropdown options
dropdown_options = {
    'period': ['1Y', '6M', '3M'],
    'analyst': list_of_unique_analysts
}

@app.route('/')
def index():
    if 'form_values' not in session:
        session['form_values'] = default_form_values
    return render_template('index.html',df=final_df, form_values=session['form_values'], dropdown_options=dropdown_options)

@app.route('/generate_data', methods=['POST'])
def generate_data():
    global calls_to_be_processed
    global final_df
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

    final_df,calls_to_be_processed = process_data(start_date, end_date, dur, analyst_to_be_displayed, l1, analyst_dfs, company_data)
    return render_template('index.html', df=final_df, form_values=form_values, dropdown_options=dropdown_options)

@app.route('/sort_table', methods=['POST'])
def sort_table():
    global final_df
    if request.method == 'POST':
        sort_by = request.form['sort_by']
        if final_df is not None and len(final_df) > 1:
            final_df = sort_data_frame(final_df, sort_by)
        return render_template('index.html', df=final_df, form_values=session['form_values'], dropdown_options=dropdown_options)

@app.route('/get_analyst_details')
def get_analyst_details():
    analyst = request.args.get('analyst')
    if analyst in calls_to_be_processed:
        details_df = calls_to_be_processed[analyst]
        details_html = details_df.to_html(classes='table table-striped')
        return jsonify({'html': details_html})
    return jsonify({'html': 'No details available for this analyst.'})

if __name__ == "__main__":
    app.run(debug=True)
