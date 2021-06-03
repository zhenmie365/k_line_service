# -*- coding: utf-8 -*-

import logging
from sqlalchemy import create_engine
from flask_oper import app

logger = logging.getLogger('index')
logger.setLevel(logging.DEBUG)

init_db_con = False

def initializer(context):
    global init_db_con
    try:
        init_db_con = create_engine("mysql+pymysql://admin_berg:Black1000@rm-wz9uxu2x6pv2z85by.mysql.rds.aliyuncs.com:3306/stock_data")
        print(type(init_db_con))
    except Exception as e:
        logger.error(e)
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")

def handler(environ, start_response):
    return app(environ, start_response)