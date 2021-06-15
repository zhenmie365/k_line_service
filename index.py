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
        init_db_con = create_engine("mysql+pymysql://lion_watch_admin:LionWatch2021@rm-bp1y26j8fi5a84hl0.mysql.rds.aliyuncs.com:3306/lion_watch_db")
        print(type(init_db_con))
    except Exception as e:
        logger.error(e)
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")

def handler(environ, start_response):
    return app(environ, start_response)