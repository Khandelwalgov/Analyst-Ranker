from flask import Flask, render_template, request
from main import load_data, process_data, sort_data_frame
from util import convert_date

app = Flask(__name__)

# Global definition of l1,analysts and company data to ensure they are loaded only once, saving time
l1, analyst_dfs, company_data = load_data()

#Global definition of final_df to make sorting easier as it wont have to be processed again everytime sorting has to be done
final_df = None

#Default route when site first loads
@app.route('/')
def index():

    return render_template('index.html')


#whenever "Generate" Button is clicked process the data and showcase final_df
@app.route('/generate_data', methods=['POST'])
def generate_data():
    if request.method == 'POST':
        global final_df

        #Take input parameters from the form
        start_date = convert_date(request.form['start-date'])
        end_date = convert_date(request.form['end-date'])
        dur = request.form['period']
        analyst_to_be_displayed = request.form['analyst']
        
        final_df = process_data(start_date, end_date, dur, analyst_to_be_displayed, l1, analyst_dfs, company_data)
        return render_template('index.html', df=final_df.to_html(classes='table table-striped'))


#Whenever sort is clicked
@app.route('/sort_table', methods=['POST'])
def sort_table():
    global final_df
    if request.method == 'POST':
        sort_by = request.form['sort_by']
        if final_df is not None and len(final_df) > 1:
            final_df = sort_data_frame(final_df, sort_by)
            return render_template('index.html', df=final_df.to_html(classes='table table-striped'))
        elif final_df is not None and len(final_df)==1:
            return render_template('index.html', df=final_df.to_html(classes='table table-striped'))
        return render_template('index.html', df=final_df)

if __name__ == "__main__":
    app.run(debug=True)
