from flask import Flask, render_template, request, redirect, url_for

from search_code import search
from search_code import check

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('nlp_main_input.html')


@app.route('/process', methods=['get'])
def search_input():
    if not request.args:
        return redirect(url_for('index'))

    line_in = request.args.get('Enter')
    results = search(request.args.get('Enter'))
    c_res = check(results)

    return render_template('nlp_main_output.html', input_line=line_in, results=results, check=c_res)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)