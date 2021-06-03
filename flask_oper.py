# coding=utf-8
from flask import Flask, request
from controller import get_hist_data_by_symbol, get_hist_data_page
from responseCode import return_error_result_6001, return_error_result_6002, return_error_result_6004, return_success_result

app = Flask(__name__)

base_path = ''

@app.route('/kLineData/symbol/<symbol>', methods=['GET'])
def kLineData(symbol):
    if not symbol:
        return return_error_result_6001([]), 400
    try:
        result = get_hist_data_by_symbol(symbol)
    except Exception as e:
        return return_error_result_6004([], msg=str(e)), 400 

    if not result:
        return return_error_result_6002(result), 400

    return return_success_result(result), 200

@app.route('/kLineData/query', methods=['GET'])
def query_hist_paging_data():
    """
    传入参数: 
    limit: int,  必须, 默认为60, 每页显示的数据量
    page: int, 必须, 默认为1, 第几页
    offset: int, 可选,默认等值于limit, 偏移量
    """
    request_body = request.args

    if not request_body or not 'symbol' in request_body:
        return return_error_result_6002([]), 400

    try:
        result = get_hist_data_page(request_body)
    except Exception as e:
        return return_error_result_6004([], msg=str(e)), 400 

    if not result:
        return return_error_result_6002(result), 400

    return return_success_result(result), 200