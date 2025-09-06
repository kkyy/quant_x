# index.py
import os
import logging
import json
import requests
from your_analysis_module import analyze_stock, fetch_data, send_wechat_message

logger = logging.getLogger()

def handler(event, context):
    logger.info("股票分析任务开始执行...")
    
    try:
        # 1. 从环境变量获取配置
        wechat_appid = os.getenv('WECHAT_APPID')
        wechat_secret = os.getenv('WECHAT_SECRET')
        wechat_template_id = os.getenv('WECHAT_TEMPLATE_ID')
        openid = os.getenv('WECHAT_OPENID')
        
        if not all([wechat_appid, wechat_secret, wechat_template_id, openid]):
            raise ValueError("微信配置不完整，请检查环境变量设置")
        
        # 2. 拉取股票数据
        logger.info("正在拉取股票数据...")
        stock_data = fetch_data()
        
        # 3. 执行量化分析
        logger.info("正在执行量化分析...")
        analysis_result = analyze_stock(stock_data)
        
        # 4. 推送微信消息
        logger.info("正在推送微信消息...")
        send_wechat_message(
            appid=wechat_appid,
            secret=wechat_secret,
            template_id=wechat_template_id,
            openid=openid,
            result=analysis_result
        )
        
        logger.info("任务执行成功！")
        return {
            'status': 'success',
            'message': '股票分析完成并成功推送微信',
            'result': analysis_result
        }
        
    except Exception as e:
        logger.error(f"任务执行出错: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }