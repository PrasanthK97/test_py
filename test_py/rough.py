import pathlib
import sys

trade_engine_folder = pathlib.Path(__file__).parent.resolve().parent.resolve()
sys.path.append(str(trade_engine_folder))
if getattr(sys, 'frozen', False):
    root_path = (
        pathlib.Path(__file__)
        .parent.resolve()
        .parent.resolve()
        .parent.resolve()
        .parent.resolve()
    )
else:
    root_path = (
        pathlib.Path(__file__)
        .parent.resolve()
        .parent.resolve()
    )

import atexit
import datetime
from decimal import Decimal
from DataController3.TradeEngine import createtdsorder, getCtWalletObject, getExWalletObject, getGioWalletObject
from DataModel3 import order_transaction, p_notify
from DataController import Wallet
import ssl
from DataModel3.binance import BINANCETRADEORDERS, BinanceExchangeWalletStatus, BinanceTradeAction, BinanceTradeOrderStatus
import websocket
from Util.utilities import truncate_decimal
from Api3P.kafka.producer import Producer
from Api3P.kafka.consumer import Consumer
import time
import traceback
import json
import yaml
import redis
import os
from DataController3.binance import createBinanceTradeOrder, getBinanceTradeOrder, updateBinanaceTradeOrder
import Util.ServiceManagement as serviceMgmt
from flask import Flask,request
import threading
import logging
from Util.LogHandler import init_logger
from Api3P import Gio
from PartnerApi import Binance
from sqlalchemy.orm import scoped_session
from sqlalchemy import text
from sqlalchemy.sql import label
from sqlalchemy import func
from struct import unpack;
from os import urandom
from collections import OrderedDict
from DataController3.utils import get_engine,init_db
from Api3P.kafka.admin import Admin
import requests
from Util.utilities import RetryOverException

app_name = "binance_service"
app = Flask(__name__)

redisClient = None
apiClients = []
apiClient1 = None
apiClient2 = None
symbolConfig = None

coinpairdata = {}
cointrancharges = {}
binanceProducer = None
binanceOrderAndEventThreadMap = {}
p_msg={}
ortype=['B','S']
config = {}
qtyAlert = None

orderPartitionCount = 0
eventPartitionCount = 0

tds_admin = None
tds_config = None

orderbookProducer = None
exchangecoinpairdata = {}
orderbook={}
orderbook_socket_url = ''
trade_socket_url = ''
decimals_fill=['%.0f','%.1f','%.2f','%.3f','%.4f','%.5f','%.6f','%.7f','%.8f','%.9f','%.10f','%.11f','%.12f','%.13f','%.14f','%.15f','%.16f','%.17f','%.18f','%.19f','%.20f']
orderBookCoinPairEventQueue = {}
orderbookSnapshotStatus = {}
refreshOrderbookSnapshotBatch = {}
thread_lock = threading.Lock()

def calibrateExchangeData(exchgdata,tzp, tzq, pairname):
    global exchangecoinpairdata, orderbook, orderbook_socket_url, trade_socket_url, orderBookCoinPairEventQueue

    exchangecoinpairdata[exchgdata['symbol']] = {}
    exchangecoinpairdata[exchgdata['symbol']]['ticksizep'] = tzp
    exchangecoinpairdata[exchgdata['symbol']]['ticksizeq'] = tzq
    exchangecoinpairdata[exchgdata['symbol']]['pairname'] = pairname

    if exchgdata['symbol'] not in orderbook:
        orderbook[exchgdata['symbol']] = {}
        orderbook[exchgdata['symbol']]['lastUpdateId'] = 0
        orderbook[exchgdata['symbol']]['bids'] = {}
        orderbook[exchgdata['symbol']]['asks'] = {}

    if pairname not in orderBookCoinPairEventQueue:
        orderBookCoinPairEventQueue[pairname] = []

    #orderbook_socket_url = "wss://stream.binance.com:9443/ws/bnbbtc@trade" 
    orderbook_socket_url = serviceMgmt.serviceConfig['binanceconfig']['orderbook']['ws_uri'] + '@depth/'.join(symbol.lower() for symbol in exchangecoinpairdata) + '@depth'
    #socket_url = "wss://stream.binance.com:9443/ws/bnbbtc@trade" 
    trade_socket_url = serviceMgmt.serviceConfig['binanceconfig']['orderbook']['ws_uri'] + '@trade/'.join(symbol.lower() for symbol in exchangecoinpairdata) + '@trade'

def calibrateCoins(session=None):
    global coinpairdata, cointrancharges, symbolConfig

    coinpairdata = {}
    cointrancharges = {}
    from DataController3.utils import session_maker
    logging.info(f"Session before if {session}")
    if session is None:
        session = scoped_session(session_maker)
    sql = text("SELECT * FROM tradeengine.cointrancharges c;")
    result = session.execute(sql)
    for row in result:
        cointrancharges.update({str(row[1]) + str(row[2]) + str(row[3]) + str(row[4]): float(row[5]) / 100})

    sql = text(f"SELECT * FROM tradeengine.coinpair_price c WHERE exchgid={serviceMgmt.serviceConfig['binanceconfig']['exchg_id']};")
    result = session.execute(sql)
    for row in result:
        try:
            coinpairdata[str(row.pairname)] = {
                "exchgid": int(row.exchgid),
                "fees": {
                    "MB": Decimal(str(row.feemb)),
                    "TB": Decimal(str(row.feetb)),
                    "MS": Decimal(str(row.feems)),
                    "TS": Decimal(str(row.feets))
                },
                "feesf": {
                    "MB": float(str(row.feemb)),
                    "TB": float(str(row.feetb)),
                    "MS": float(str(row.feems)),
                    "TS": float(str(row.feets))
                }
            }

            coinpairdata[row.pairname]['EXGDT1'] = {}
            coinpairdata[row.pairname]['EXGDT2'] = {}
            coinpairdata[row.pairname]['TDS'] = [Decimal(str(row.tdsp)),Decimal(str(row.tdsb))]
            coinpairdata[row.pairname]['TDSP'] = Decimal(str(row.tdsp))
            coinpairdata[row.pairname]['TDSB'] = Decimal(str(row.tdsb))
            coinpairdata[row.pairname]["TZP"] = int(row.ticksizep)
            coinpairdata[row.pairname]["TZQ"] = int(row.ticksizeq)
            coinpairdata[row.pairname]['EXGDT1'] = json.loads(str(row.exchgdata1))
            coinpairdata[row.pairname]['EXGDT2'] = json.loads(str(row.exchgdata2))

            logging.info(str(coinpairdata[row.pairname]['EXGDT1']))
            symbolConfig.append(coinpairdata[row.pairname]['EXGDT1']['symbol'])

            calibrateExchangeData(coinpairdata[row.pairname]['EXGDT1'], coinpairdata[row.pairname]["TZP"], coinpairdata[row.pairname]["TZQ"], row.pairname)
        except:
            logging.exception("calibrateCoins")

    if session:
        session.close()

    symbolConfig = list(set(symbolConfig))
    logging.info(f"CoinPairData - {coinpairdata}")
    logging.info(f"CoinTranCharges - {cointrancharges}")

def calibrateTdsConfigs(session=None):
    global tds_admin, tds_config    
    tds_config = serviceMgmt.serviceConfig['tds_config']
    logging.info(f"TDS CONFIG : {tds_config}")
    tds_admin = tds_config["ctid"] 
    logging.info(f"TDS ADMIN ACCOUNT : {tds_admin}") 
    
    
def calibrateNotification(session=None):
    global p_msg
    from DataController3.utils import session_maker
    if session is None:
        session = scoped_session(session_maker)
    sql = text("SELECT p.key, p.msg FROM notification.p_msg p;")
    result = session.execute(sql)
    for row in result:
        p_msg[row.key] = row.msg
    logging.info(f"Notification messages: {p_msg}")

def calibrate_lowbalance_alert_config(session=None):
    global qtyAlert
    qtyAlert = serviceMgmt.serviceConfig['binanceconfig']['binance_lowbalance_alert']

def calibrateMessageQueue(session=None):
    admin = Admin(kafkaServer=serviceMgmt.serviceConfig["kafka_config"]["server"])
    if admin.getPartitionCount(serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic']) < serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_partition']:
        admin.createTopicPartitons(serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_partition'])
        logging.info("Event Partitions are added successfully.")
    else:
        logging.info("Existing event partitions are more than the required partition. So cannot reduce.")

    if admin.getPartitionCount(serviceMgmt.serviceConfig['binanceconfig']['orderbook']['kafka']['buffer_topic']) < serviceMgmt.serviceConfig['binanceconfig']['orderbook']['kafka']['buffer_partition']:
        admin.createTopicPartitons(serviceMgmt.serviceConfig['binanceconfig']['orderbook']['kafka']['buffer_topic'],serviceMgmt.serviceConfig['binanceconfig']['orderbook']['kafka']['buffer_partition'])
        logging.info("Buffer Orderbook Partitions are added successfully.")
    else:
        logging.info("Existing buffer Orderbook partitions are more than the required partition. So cannot reduce.")

def notify_order_rejection(coin="",ctId ="", amount="", orderId="",clientOrderId="",orderType="",tranType="",msg="",errorCode = ""):
    alert_config = serviceMgmt.serviceConfig["binanceconfig"]['alerts']
    alert = alert_config['alert_for']['order_rejection']
    if alert_config and alert['enabled']:
        if alert['sendvia'] == 'email':
            mailing_list = alert_config['email']["to"]
            logging.info("Sending emails to {} for Order rejection in binance ".format(json.dumps(mailing_list)))
            for email in mailing_list:
                Gio.submitEmailNotification(serviceMgmt.appInstanceUrlMap,to=str(email),key= "order_rejection_binance",
                                                       value={"coin":coin, "ctId": ctId, "amount": amount, "orderId":orderId if orderId is not None else "-",
                                                              "clientOrderId":clientOrderId if clientOrderId is not None else "-",
                                                              "orderType":orderType if orderId is not None else "-",
                                                              "tranType":tranType if orderId is not None else "-",
                                                              "errorCode":errorCode,"msg":msg},appName=app_name) 

def notify_low_balance(coin="", msg="",currentBalance="",expectedBalance=""):
    alert_config = serviceMgmt.serviceConfig["binanceconfig"]['alerts']
    alert = alert_config['alert_for']['low_balance']
    if alert_config and alert['enabled']:
        if alert['sendvia'] == 'email':
            mailing_list = alert_config['email']["to"]
            logging.info("Sending emails to {} for Low balance in binance ".format(json.dumps(mailing_list)))
            for email in mailing_list:
                Gio.submitEmailNotification(serviceMgmt.appInstanceUrlMap,to=str(email),key= "binance_low_balance",
                                            value={"coin":coin,"msg":msg,"currentBalance":currentBalance,"expectedBalance":expectedBalance},appName=app_name)             


def calibrateAppDataAndConfigCallback():
    global apiClient1, apiClient2, symbolConfig, apiClients
    apiClients = []
    # config = serviceMgmt.serviceConfig["binanceconfig"]
    # apiClient1 = Binance.Binance(config=config)
    # config = dict(config)
    symbolConfig = serviceMgmt.serviceConfig['binanceconfig']['binance_api_symbols']
    # config["api_key"] = config["api_key2"]
    # config["api_secret"] = config["api_secret2"]
    # apiClient2 = Binance.Binance(config=config)
    for coinpair in coinpairdata:
        symbolConfig.append(coinpairdata[coinpair]['EXGDT1']['symbol'])

    symbolConfig = list(set(symbolConfig))

    for config in serviceMgmt.serviceConfig["binanceconfig"]["api_cred"]:
        if serviceMgmt.serviceConfig["binanceconfig"]["proxy"] is not None:
            proxyconfig = serviceMgmt.serviceConfig["proxy_server"][serviceMgmt.serviceConfig["binanceconfig"]["proxy"]]
            ip = proxyconfig["ip"]
            port = proxyconfig["port"]
            config["proxy"] = {"http": f"http://{ip}:{port}","https": f"http://{ip}:{port}"}
        config["api_url"] = serviceMgmt.serviceConfig["binanceconfig"]["api_url"]
        apiClients.append(Binance.Binance(config=config))

def shutdownhook():
    serviceMgmt.shutdownHook()

@serviceMgmt.app.before_request
def before_request():
    res = serviceMgmt.validateBeforeRequest()
    if res is not None:
        return res

def getAccount(account):
    account = int(account)-1
    logging.info("Account:{}".format(account))
    # if account == 1:
    #     return apiClient1
    # elif account == 2:
    #     return apiClient2
    if account <= len(apiClients)-1 and account >= 0:
        return apiClients[account]
@serviceMgmt.app.route("/balance/<int:account>", methods=["GET"])
def getBalance(account=1):
    try:
        apiClient = getAccount(account=account)
        res = apiClient.getWalletBalance()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getBalance')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/spot/balance", methods=["GET"])
def getBalanceByMappedAccount():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
        res = apiClient.getWalletBalance()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getBalance')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/spot/openorders", methods=["GET"])
def getAllOpenOrders():
    try:
        account = request.args.get("account")
        if account is None:
            account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade']
        apiClient = getAccount(account=account)
        symbol = request.args.get("symbol")
        openOrders = {}
        if symbol=="all":
            response = apiClient.getAllOpenOrders()
            return json.dumps({'Status': 'Success', 'Data': response})
        elif symbol=="config":
            size = len(symbolConfig)
            count = size - 1
            while count >= 0:
                try:
                    logging.info("Getting:{}/{}".format(count, size))
                    symbol = symbolConfig[count]
                    res = apiClient.getAllOpenOrders(symbol=symbol)
                    if len(res)>0:
                        openOrders[symbol] = res
                    count = count - 1
                except RetryOverException:
                    logging.exception("getAllOpenOrders") 
                    break   
                except Exception:
                    time.sleep(5)
                    logging.exception("Wait:{}".format(count))
            logging.info("Complted:{}".format(size))
            return json.dumps({'Status': 'Success', 'Data': openOrders})
        else:
            res = apiClient.getAllOpenOrders(symbol=symbol)
            return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getBalance')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/savingflexbalance/<int:account>", methods=["GET"])
def getSavingFlexBalance(account=1):
    try:
        apiClient = getAccount(account=account)
        res = apiClient.getSavingFlexBalance()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getSavingFlexBalance')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/savingfixedbalance/<int:account>", methods=["GET"])
def getSavingFixedBalance(account=1):
    try:
        apiClient = getAccount(account=account)
        res = apiClient.getSavingFixedBalance()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getSavingFixedBalance')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/stakingbalance/<int:account>", methods=["GET"])
def getStakingBalance(account=1):
    try:
        apiClient = getAccount(account=account)
        res = apiClient.getStakingBalance()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getStakingBalance')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/trades/<int:account>", methods=["POST","GET"])
def getTrades(account=1):
    try:
        # Using Different proxy
        config = serviceMgmt.serviceConfig["binanceconfig"]["api_cred"][account-1]
        if serviceMgmt.serviceConfig["binanceconfig"]["proxy_to_fetch_trades"] is not None:
            proxyconfig = serviceMgmt.serviceConfig["proxy_server"][serviceMgmt.serviceConfig["binanceconfig"]["proxy_to_fetch_trades"]]
            ip = proxyconfig["ip"]
            port = proxyconfig["port"]
            config["proxy"] = {"http": f"http://{ip}:{port}","https": f"http://{ip}:{port}"}
        config["api_url"] = serviceMgmt.serviceConfig["binanceconfig"]["api_url"]
        apiClient = Binance.Binance(config=config)
        
        args = request.args
        symbol = args["symbol"]
        limit=None
        startTime = None
        endTime = None
        trades={}
        if 'startTime' in args:
            startTime = args['startTime']
        if 'endTime' in args:
            endTime = args['endTime']
        if 'limit' in args:
            limit = args["limit"]
        startTimeDict = {}
        if request.method == "POST":
            request_data = request.get_json(force=True)
            startTimeDict = request_data['startTimeDict']
        if symbol=="all":
            if len(startTimeDict) == 0:
                res = apiClient.getAllSymbols()
                symbols = res["symbols"]
                size = len(symbols)
                count = size-1
            else:
                symbols = list(startTimeDict.keys())
                size = len(symbols)
                count = size-1
            
            while count>=0:
                try:
                    logging.info("Getting:{}/{}".format(count,size))
                    symbol = symbols[count]["symbol"] if len(startTimeDict) == 0 else symbols[count]
                    if symbol in startTimeDict:
                        startTime = startTimeDict[symbol]
                    res = apiClient.getTrades(symbol=symbol, limit=limit, startTime=startTime, endTime=endTime)
                    trades[symbol] = res
                    count=count-1
                    time.sleep(1)
                except RetryOverException:
                    logging.exception("getTrades") 
                    break 
                except Exception:
                    logging.exception("Wait:{}".format(count))
                    time.sleep(5)
            return json.dumps({'Status': 'Success', 'Data': trades})
        elif symbol=="config":
            if len(startTimeDict) == 0:
                size = len(symbolConfig)
                count = size - 1
            else:
                symbols = list(startTimeDict.keys())
                size = len(symbols)
                count = size - 1
            while count >= 0:
                try:
                    logging.info("Getting:{}/{}".format(count, size))
                    symbol = symbolConfig[count] if len(startTimeDict) == 0 else symbols[count]
                    if symbol in startTimeDict:
                        startTime = startTimeDict[symbol]
                    res = apiClient.getTrades(symbol=symbol, limit=limit, startTime=startTime, endTime=endTime)
                    if len(res)>0:
                        trades[symbol] = res
                    count = count - 1
                    time.sleep(1)
                except RetryOverException:
                    logging.exception("getTrades") 
                    break
                except Exception:
                    time.sleep(5)
                    logging.exception("Wait:{}".format(count))
            logging.info("Complted:{}".format(size))
            return json.dumps({'Status': 'Success', 'Data': trades})
        else:
            res = apiClient.getTrades(symbol=symbol, limit=limit, startTime=startTime, endTime=endTime)
            return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getTrades')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/spot/coinpairs", methods=["GET"])
def getTradeEnabledCoinpairs():
    try:
        return json.dumps({'Status': 'Success', 'Data': symbolConfig})
    except:
        logging.exception("Error")
@serviceMgmt.app.route("/spot/trades", methods=["GET"])
def getTradesByMappedAccount():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
        args = request.args
        symbol = args["symbol"]
        limit=None
        trades={}
        if 'limit' in args:
            limit = args["limit"]
        if symbol=="all":
            res = apiClient.getAllSymbols()
            symbols = res["symbols"]
            size = len(symbols)
            count = size-1
            while count>=0:
                try:
                    logging.info("Getting:{}/{}".format(count,size))
                    symbol = symbols[count]["symbol"]
                    res = apiClient.getTrades(symbol=symbol, limit=limit)
                    trades[symbol] = res
                    count=count-1
                    time.sleep(0.1)
                except RetryOverException:
                    logging.exception("getTradesByMappedAccount") 
                    break
                except Exception:
                    logging.exception("Wait:{}".format(count))
                    time.sleep(5)
            return json.dumps({'Status': 'Success', 'Data': trades})
        elif symbol=="config":
            size = len(symbolConfig)
            count = size - 1
            while count >= 0:
                try:
                    logging.info("Getting:{}/{}".format(count, size))
                    symbol = symbolConfig[count]
                    res = apiClient.getTrades(symbol=symbol, limit=limit)
                    if len(res)>0:
                        trades[symbol] = res
                    count = count - 1
                    time.sleep(0.1)
                except RetryOverException:
                    logging.exception("getTradesByMappedAccount") 
                    break
                except Exception:
                    time.sleep(5)
                    logging.exception("Wait:{}".format(count))
            logging.info("Complted:{}".format(size))
            return json.dumps({'Status': 'Success', 'Data': trades})
        else:
            res = apiClient.getTrades(symbol=symbol, limit=limit)
            return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getTrades')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/transfers/<int:account>", methods=["GET"])
def getTransfers(account=1):
    try:
        apiClient = getAccount(account=account)
        params = {}

        transfer_type = request.args.get('type')
        if transfer_type is not None and (not isinstance(transfer_type, str) or (transfer_type != 'deposit' and transfer_type != 'withdraw')):
            return json.dumps({'Status': 'Error', 'Msg': 'Invalid value given for type. Enter deposit or withdraw'})
        if 'startTime' in request.args:
            params['startTime'] = request.args['startTime']
        if 'endTime' in request.args:
            params['endTime'] = request.args['endTime']

        dparams = json.loads(json.dumps(params))
        wparams = json.loads(json.dumps(params))

        dStartTime = request.args.get("dStartTime")
        if dStartTime is not None:
            dparams['startTime'] = dStartTime
        wStartTime = request.args.get("wStartTime")
        if wStartTime is not None:
            wparams['startTime'] = wStartTime

        res = apiClient.getTransfers(type=transfer_type, dparams=dparams, wparams=wparams)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getTransfers')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})
    
@serviceMgmt.app.route("/spot/transfers", methods=["GET"])
def getTransfersByMappedAccount():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
        res = apiClient.getTransfers()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getTransfers')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/lending/project/list/<int:account>", methods=["GET"])
def getLendingProjectList(account=1):
    try:
        ptype = request.args.get('type')
        if ptype is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [type] is required'})

        apiClient = getAccount(account=account)
        res = apiClient.getLendingProjectList(ptype)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getLendingProjectList')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/lending/project/position/list/<int:account>", methods=["GET"])
def getLendingProjectPositionList(account=1):
    try:
        apiClient = getAccount(account=account)
        res = apiClient.getLendingProjectPositionList()
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getLendingProjectPositionList')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/lending/union/purchaseRecord/<int:account>", methods=["GET"])
def getLendingUnionPurchaseRecords(account=1):
    try:
        ltype = request.args.get('type')
        if ltype is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [type] is required'})

        apiClient = getAccount(account=account)
        res = apiClient.getLendingUnionPurchaseRecords(ltype)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getLendingUnionPurchaseRecords')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/lending/union/redemptionRecord/<int:account>", methods=["GET"])
def getLendingUnionRedemptionRecords(account=1):
    try:
        ltype = request.args.get('type')
        if ltype is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [type] is required'})

        apiClient = getAccount(account=account)
        res = apiClient.getLendingUnionRedemptionRecords(ltype)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getLendingUnionRedemptionRecords')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/lending/union/interestHistory/<int:account>", methods=["GET"])
def getLendingUnionInterestHistory(account=1):
    try:
        ltype = request.args.get('type')
        if ltype is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [type] is required'})

        apiClient = getAccount(account=account)
        res = apiClient.getLendingUnionInterestHistory(ltype)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getLendingUnionInterestHistory')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/staking/position/<int:account>", methods=["GET"])
def getStakingPosition(account=1):
    try:
        product = request.args.get('product')
        if product is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [product] is required'})

        apiClient = getAccount(account=account)
        res = apiClient.getStakingPosition(product)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getStakingPosition')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})

@serviceMgmt.app.route("/staking/stakingRecord/<int:account>", methods=["GET"])
def getStakingRecord(account=1):
    try:
        product = request.args.get('product')
        if product is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [product] is required'})

        txnType = request.args.get('txnType')
        if txnType is None:
            return json.dumps({'Status': 'Error', 'Msg': 'Param [txnType] is required'})
        
        page = request.args.get('page')

        apiClient = getAccount(account=account)
        res = apiClient.getStakingRecord(product, txnType, page)
        return json.dumps({'Status': 'Success', 'Data': res})
    except Exception as e:
        logging.exception('getStakingRecord')
        return json.dumps({'Status': 'Error', 'Msg': str(e)})
    
@serviceMgmt.app.route("/getmytrades",methods=["POST"])   
def getmytrades():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
        request_data = request.get_json()
        result = apiClient.getTradesByOrder(symbol=request_data['symbol'],orderId = request_data['orderId']) # we can mention the fromid to get the trade history
        return json.dumps({"Status":"Success","Msg":result})
    except Exception as e:
        logging.exception(f'Unable to get my trade due to {str(e)}')
        return json.dumps({"Status":"Error","Msg":str(e)})

@serviceMgmt.app.route("/createsubaccount",methods=["POST"])
def createSubAccount():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['main_account'])
        subAccountString = request.args.get("subAccountString")
        if 0 > len(subAccountString) > 31:
            return json.dumps({"Status":"Error","Msg":"Invalid Parameters"}) 
        # This will create a virtual email using subAccountString for you to register under master account
        # You need to enable "trade" option for the API Key which requests this endpoint.
        response = apiClient.createSubAccount(subAccountString=subAccountString)
        return json.dumps({"Status":"Success","Msg":response}) 
    except Exception as e:
        logging.exception(f'Unable to create sub account due to {str(e)}')
        return json.dumps({"Status":"Error","Msg":str(e)})
    
@serviceMgmt.app.route("/createsubaccountapikeys",methods=["POST"])
def createSubAccountApiKey():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['main_account'])
        subAccountId = request.args.get("subAccountId")
        canTrade = request.args.get("canTrade")
        if subAccountId is None or canTrade is None:
            return json.dumps({"Status":"Error","Msg":"Invalid Parameters"}) 
        response = apiClient.createSubAccountApiKey(subAccountId=str(subAccountId), canTrade=canTrade)
        return json.dumps({"Status":"Success","Msg":response}) 
    except Exception as e:
        logging.exception(f'Unable to create sub account api keys due to {str(e)}')
        return json.dumps({"Status":"Error","Msg":str(e)})
    
@serviceMgmt.app.route("/listsubaccount",methods=["GET"])
def listSubAccount():
    try:
        apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['main_account'])
        response = apiClient.listSubAccount()
        return json.dumps({"Status":"Success","Msg":response}) 
    except Exception as e:
        logging.exception(f'Unable to list sub account due to {str(e)}')
        return json.dumps({"Status":"Error","Msg":str(e)})

@serviceMgmt.app.route("/transferFund",methods=["POST"])
def transferFund():
    try:
        sweep_out = request.args.get("sweepOut")
        sweep_in = request.args.get("sweepIn")
        symbol = request.args.get("symbol")
        amount = request.args.get("amount")
        # You need to enable "internal transfer" option for the api key which requests this endpoint.
        # Transfer from master account if fromId not sent.
        # Transfer to master account if toId not sent.
        if sweep_out is None and sweep_in is None:
            return json.dumps({"Status":"Error","Msg":"Either From Id or to id must present"}) 
        if symbol is None:
            return json.dumps({"Status":"Error","Msg":"Symbol is mandatory"})
        if amount is None:
            return json.dumps({"Status":"Error","Msg":"Amount is mandatory"})
        params = {}
        if sweep_out is not None:
            apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
            params['fromId'] = str(sweep_out)
        if sweep_in is not None:
            apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['main_account'])
            params['toId'] = str(sweep_in)
        params['asset'] = symbol
        params['amount'] = Decimal(amount)
        response = apiClient.transferFund(params=params)
        return json.dumps({"Status":"Success","Msg":response}) 
    except Exception as e:
        logging.exception(f'Unable to create sub account api keys due to {str(e)}')
        return json.dumps({"Status":"Error","Msg":str(e)})

@serviceMgmt.app.route("/getorderbook/<string:coinpair>",methods=["GET"])
def getOrderBookData(coinpair):
    try:
        if coinpair not in orderbook:
            return json.dumps({'Status': 'Error', 'Msg': "Invalid coinpair"})
        orderbooktemp = {coinpair:{'bids':{},'asks':{}}}
        for order in orderbook[coinpair]['bids']:
            orderbooktemp[coinpair]["bids"][str(order)] = orderbook[coinpair]['bids'][order]
        for order in orderbook[coinpair]['asks']:
            orderbooktemp[coinpair]["asks"][str(order)] = orderbook[coinpair]['asks'][order]
        return json.dumps({'Status': 'Success', 'Data': json.dumps(orderbooktemp[coinpair])})
    except Exception as e:
        logging.exception(f'Unable to fetch orderbook due to {str(e)}')
        return json.dumps({"Status":"Error","Msg":str(e)})

def checkAccountBalance(apiClient=None):
    while True:
        logging.info(f"Breakers process_checkbalance {get_service_info()['breakers']['process_check_balance']}")
        if get_service_info()["breakers"]["process_check_balance"]:
            try:
                res = apiClient.getWalletBalance()
                for coin in res['balances']:
                    logging.info(f"Coin - {coin['asset']} Free {coin['free']}  Locker {coin['locked']}")
                    if coin['asset'] in qtyAlert and Decimal(coin['free']) <= qtyAlert[coin['asset']]['min_qty']:
                        notify_low_balance(coin=coin['asset'],currentBalance=coin['free'],expectedBalance=qtyAlert[coin['asset']]['min_qty'],msg="Low Balance")
                        logging.info(f"Low balance in binance for {coin['asset']} ")
            except Exception as e:
                logging.exception(f"Unable to get the account balance due to {str(e)}")
        time.sleep(serviceMgmt.serviceConfig["binanceconfig"]['process_check_balance_sleep_in_secs'])
    
# To check isMakerOrder
# def isMakerOrder(orderId,symbol,tradeId):
#     try:
#         apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
#         logging.info(f"Getting trades for order id {orderId} symbol {coinpairdata[symbol]['EXGDT1']['symbol']}")
#         result = apiClient.getTradesByOrder(symbol = coinpairdata[symbol]['EXGDT1']['symbol'],orderId = orderId)
#         logging.info(f"isMaker trade list {result}")
#         for row in result:
#             if int(tradeId) == int(row['id']):
#                 return row['isMaker']
#         return False
#     except Exception as e:
#         logging.exception(f"Unable to get the trades for order due to {str(e)}")

def updateOrderCreationData(request=None,response=None):
    createData = [
                {
                "createRequest":{
                    "newClientOrderId" : str(request['orderid']),
                    "symbol" : coinpairdata[request['coinpair']]['EXGDT1']['symbol'],
                    "side" : "BUY" if request['ordertype'] == 0 else "SELL",
                    "type" : "LIMIT",
                    "timeInForce" : "GTC",
                    "quantity" : request['amount'],
                    "price" : request['price']
                },
                "createResponse": response
            }
        ]
    updateBinanaceTradeOrder(clientorderid = request["orderid"],createdata=json.dumps(createData))

def updateOrderCancellationData(request=None,response=None):
    cancelData = {
        "cancelRequest":{
            "orderId" :  request['orderId'],
            "symbol" : coinpairdata[request['coinpair']]['EXGDT1']['symbol'],
            "origClientOrderId" : request['clientOrderId']
        },
        "cancelResponse": response
    }
    order = getBinanceTradeOrder(clientorderid=request['clientOrderId'])
    data = json.loads(order['createdata'])
    data.append(cancelData)
    updateBinanaceTradeOrder(clientorderid = request['clientOrderId'],createdata=json.dumps(data))

def transferTruncationDust(freezed, order, ctwalletbase, exwalletbase, db_session):
    logging.info(f"E)Checking order.freeze {order.freezed} frezzed: {freezed}")
    if freezed > 0.0 and freezed == Decimal(str(order.freezed)) and order.ordertype == 0:
        logging.info(f"G)Checking order.freeze {order.freezed}")
        ctwalletbase.freezed = truncate_decimal(str(ctwalletbase.freezed)) - freezed
        exwalletbase.freezed = truncate_decimal(str(exwalletbase.freezed)) - freezed
        adminaccountwalletbase = getCtWalletObject(ctid = serviceMgmt.serviceConfig["binanceconfig"]['admin_acc_id'], curr = order.basecoin, dbsession = db_session)
        adminaccountwalletbase.value = truncate_decimal(str(adminaccountwalletbase.value)) + freezed
        order.freezed = truncate_decimal(str(order.freezed)) - freezed
    logging.info(f"F)Checking order.freeze {order.freezed}")

#Kafka Consumer for orders
def processOrderConsumer(partitionID=None,apiClient=None):
    logging.info(f"Process Order Consumer Started for partitionID - {partitionID}")
    try:
        consumer = Consumer(
            kafkaServer=serviceMgmt.serviceConfig["kafka_config"]["server"],
            clientId=app_name,
            autoOffsetReset=serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_consumer_auto_offset_reset'],
            topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_topic'],
            groupId = serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_group_id'],
            groupInstanceId='{}_{}_{}'.format(serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_group_id'], int(time.time()),partitionID),
            autoCommit=serviceMgmt.serviceConfig["binanceconfig"]['kafka']["enable_order_auto_commit"]
        )
        while True:
            if get_service_info()["breakers"]["process_order_consumer"]:
                message = consumer.consumeMessage()
                if message is not None:
                    logging.info(f"type - {type(message)} data -> {message}")
                    message = json.loads(message)
                    binanceOrder = None
                    orderID = None
                    try:
                        if message['eventName'] == "CREATE_ORDER":
                            logging.info(f"Message from consumer -> {message}")
                            logging.info(f"+++++ Tran type { message['ordertype']} { message['trantype']} orderid {str(message['orderid'])}")
                            orderCreated = createBinanceTradeOrder(
                                clientid = message['clientid'],
                                clientorderid = str(message["orderid"]),
                                price = message['price'],
                                amount = message['amount'],
                                amount_g = message['amount_g'],
                                freezed = message['freezed'],
                                status = BinanceTradeOrderStatus.NEW,
                                ordertype = message['ordertype'],
                                trantype =  message['trantype'],
                                tds = message['tds'],
                                coinpair = str(message['coinpair']),
                                basecoin = str(message['basecoin']),
                                paircoin = str(message['paircoin']),
                                exchgid = serviceMgmt.serviceConfig['binanceconfig']['exchg_id'],
                                exwallet_status = 0,
                                tradeorderid = int(message['id']),
                                action = BinanceTradeAction.CREATE,
                                updatedtime = datetime.datetime.now()
                            )
                            logging.info(f"Consumed and created data in binance trade order table successfully---> {orderCreated}")
                            consumer.commit()
                            if orderCreated:
                                binanceOrder = apiClient.createOrder(
                                    newClientOrderId = str(message['orderid']),
                                    symbol = coinpairdata[message['coinpair']]['EXGDT1']['symbol'],
                                    side = "BUY" if message['ordertype'] == 0 else "SELL",
                                    type = "LIMIT",
                                    timeInForce = "GTC",
                                    quantity = message['amount'],
                                    price = message['price']
                                )
                                updateOrderCreationData(request=message,response=binanceOrder)
                                logging.info('Updated the binance response')
                                binanceOrder['eventType'] = 'httpEvent'
                                binanceOrder['eventName'] = binanceOrder['status']
                                orderID = binanceOrder['orderId']
                                partitionId = int(message["orderid"]) % eventPartitionCount
                                logging.info(f'PartitionId : {partitionId}  Orderid : {message["orderid"]}')
                                binanceProducer.produceMessage(
                                    data=json.dumps(binanceOrder),
                                    topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                                    partition = partitionId,
                                    key = "httpEvent"
                                )
                                logging.info(f"Consumer_process_order - eventName : CREATE_ORDER - data -> {binanceOrder}")
                        
                        if message['eventName'] == "PLACE_ORDER":
                            consumer.commit()
                            try:
                                binanceOrderDetail = apiClient.getOrder(symbol = coinpairdata[message['coinpair']]['EXGDT1']['symbol'],origClientOrderId = str(message['orderid']))
                            except Exception as e:
                                # Response when order does not exist -> {"code":-2013,"msg":"Order does not exist."}
                                error = str(e).split(":",2)
                                error = json.loads(str(error[2]))
                                if len(error) > 2 and str(error['code']) == '-2013':
                                    binanceOrder = apiClient.createOrder(
                                        newClientOrderId = str(message['orderid']),
                                        symbol = coinpairdata[message['coinpair']]['EXGDT1']['symbol'],
                                        side = "BUY" if message['ordertype'] == 0 else "SELL",
                                        type = "LIMIT",
                                        timeInForce = "GTC",
                                        quantity = message['amount'],
                                        price = message['price']
                                    )
                                    updateOrderCreationData(request=message,response=binanceOrder)
                                    binanceOrder['eventType'] = 'httpEvent'
                                    binanceOrder['eventName'] = binanceOrder['status']
                                    orderID = binanceOrder['orderId']
                                    partitionId = int(message["orderid"]) % eventPartitionCount
                                    logging.info(f'PartitionId : {partitionId}  Orderid : {message["orderid"]}')
                                    binanceProducer.produceMessage(
                                        data=json.dumps(binanceOrder),
                                        topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                                        partition = partitionId,
                                        key = "httpEvent"
                                    )
                                    logging.info(f"Consumer_process_order - eventName : PLACE_ORDER - data -> {binanceOrder}")
                                else:
                                    raise Exception(e)
                                
                        if message['eventName'] == "CANCEL_ORDER":
                            consumer.commit()
                            logging.info(f"Message from consumer in cancel order -> {message}")
                            binanceOrder = apiClient.cancelOrder(
                                orderId =  message['orderId'],
                                symbol = coinpairdata[message['coinpair']]['EXGDT1']['symbol'],
                                origClientOrderId = message['clientOrderId']
                            )
                            updateOrderCancellationData(request=message,response=binanceOrder)
                            logging.info(f"updated in binancetradeorder table")
                            binanceOrder['eventType'] = 'httpEvent'
                            binanceOrder['eventName'] = binanceOrder['status']
                            binanceOrder['clientOrderId'] = binanceOrder['origClientOrderId']
                            orderID = binanceOrder['orderId']
                            partitionId = int(message['clientOrderId']) % eventPartitionCount
                            logging.info(f'PartitionId : {partitionId}  Orderid : {message["orderId"]}')
                            binanceProducer.produceMessage(
                                data=json.dumps(binanceOrder),
                                topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                                partition = partitionId,
                                key = "httpEvent"
                            )
                            logging.info(f"Consumer_process_order - eventName : CANCEL_ORDER - data -> {binanceOrder}")
                        
                        if message['eventName'] == "REJECT_ORDER":
                            logging.info(f"Message from consumer -> {message}")
                            consumer.commit() 
                            orderID = message['clientOrderId']
                            partitionId = int(message['clientOrderId']) % eventPartitionCount
                            message['eventType'] = "internalEvent"
                            logging.info(f'PartitionId : {partitionId}  Orderid : {message["clientOrderId"]}')
                            binanceProducer.produceMessage(
                                data=json.dumps(message),
                                topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                                partition = partitionId,
                                key = "internalEvent"
                            ) 
                            logging.info(f"Consumer_process_order - eventName : REJECT_ORDER - data -> { message['clientOrderId']}")
                        
                    except Exception as e:
                        #Request error:400:{"code":-2010,"msg":"Account has insufficient balance for requested action."}
                        error = str(e).split(":",2)
                        if len(error) > 2:
                            try:
                                error = json.loads(str(error[2]))
                                notify_order_rejection(
                                    coin=message['coinpair'],
                                    ctId = message['ctId'],
                                    amount = message["amount"],
                                    orderId=message['orderId'] if "orderId" in message else "-",
                                    clientOrderId=message['clientOrderId'] if "clientOrderId" in message else "-",
                                    orderType=message['ordertype'] if "ordertype" in message else "-",
                                    tranType=message['trantype'] if "trantype" in message else "-",
                                    msg=error['msg'],
                                    errorCode = error['code']
                                )
                                logging.info(f"Order Exception : {message}")
                            except:
                                logging.exception("processOrderConsumer - JSON parse error during exception handling")

                        if message['eventName'] in ["CREATE_ORDER","PLACE_ORDER"]:
                            message['clientOrderId'] = str(message["orderid"])
                            orderID = message['clientOrderId']
                            partitionId = int(message['clientOrderId']) % eventPartitionCount
                            message['eventType'] = "internalEvent"
                            message['status'] = "FILTERS_FAILURE"
                            logging.info(f'PartitionId : {partitionId}  Orderid : {message["clientOrderId"]}')
                            binanceProducer.produceMessage(
                                data=json.dumps(message),
                                topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                                partition = partitionId,
                                key = "internalEvent"
                            )
                            logging.exception(f"Unable to process order {orderID}")
            time.sleep(serviceMgmt.serviceConfig['binanceconfig']['processOrderConsumerSleepInSeconds'])
    except Exception:
        logging.exception(f"Unable to process order consumer")        
           
#Kafka Consumer for events            
def processEventConsumer(partitionID=None):
    logging.info(f"Process Event Consumer Started for partitionID - {partitionID}")
    try:
        from DataController3.utils import session_maker
        db_session = scoped_session(session_maker)
        consumer = Consumer(
            kafkaServer=serviceMgmt.serviceConfig["kafka_config"]["server"],
            clientId=app_name,
            autoOffsetReset=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_consumer_auto_offset_reset'],
            topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
            groupId = serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_group_id'],
            groupInstanceId='{}_{}_{}'.format(serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_group_id'], int(time.time()),partitionID),
            autoCommit=serviceMgmt.serviceConfig["binanceconfig"]['kafka']["enable_event_auto_commit"]
        )
        clientOrderId = None
        while True:
            if get_service_info()["breakers"]["process_event_consumer"]:
                message = consumer.consumeMessage()    
                if message is not None:
                    consumer.commit()
                    logging.info(f"type - {type(message)} data -> {message}")
                    message = json.loads(message)
                    logging.info(f"Order data -> {message}")
                    errorMsg = None
                    try:
                        clientOrderId = message['clientOrderId']
                        orderDetail = db_session.query(BINANCETRADEORDERS).filter(BINANCETRADEORDERS.clientorderid == clientOrderId).with_for_update().one()
                        
                        if orderDetail.exwallet_status == BinanceExchangeWalletStatus.NOT_INSERTED:
                            exwalletbase = getExWalletObject(exchangeid=1, coin=orderDetail.basecoin,db_session=db_session)
                            exwalletpair = getExWalletObject(exchangeid=1, coin=orderDetail.paircoin,db_session=db_session)
                            logging.info(f"Before insert in binancetradeorder -> exwalletbase.freezed : {exwalletbase.freezed} exwalletbase.value:{exwalletbase.value} ")
                            if orderDetail.ordertype == 0:
                                exwalletbase.freezed = Decimal(str(exwalletbase.freezed)) + Decimal(str(orderDetail.freezed))
                                exwalletbase.value = Decimal(str(exwalletbase.value)) - Decimal(str(orderDetail.freezed))
                            else:
                                exwalletpair.freezed = Decimal(str(exwalletpair.freezed)) + Decimal(str(orderDetail.freezed))
                                exwalletpair.value = Decimal(str(exwalletpair.value)) - Decimal(str(orderDetail.freezed))
                            logging.info(f"After insert in binancetradeorder -> exwalletbase.freezed : {exwalletbase.freezed} exwalletbase.value:{exwalletbase.value} ")
                            orderDetail.exwallet_status=BinanceExchangeWalletStatus.INSERTED
                            db_session.flush()
                            db_session.commit() 
                        if message['status'] == "NEW": 
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")
                            logging.info(f"orderDetail status {orderDetail.status} clientid {orderDetail.clientid} amount {orderDetail.amount} freezed {orderDetail.freezed}")
                            if orderDetail.status == BinanceTradeOrderStatus.NEW:
                                orderDetail.exchgorderid = message['orderId']
                                orderDetail.status= BinanceTradeOrderStatus.ORDER_PLACED
                                orderDetail.updatedtime = datetime.datetime.now()
                            db_session.flush()
                            db_session.commit()   
                            
                        if message['status'] == 'PARTIALLY_FILLED':
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")                    
                            trades = json.loads(json.dumps(message['fills']))
                            trades = [{**trade, 'isMaker' : False} if 'isMaker' not in trade else trade for trade in trades]
                            if orderDetail.status in [BinanceTradeOrderStatus.NEW,BinanceTradeOrderStatus.ORDER_PLACED,BinanceTradeOrderStatus.PARTIALLY_FILLED]:
                                status = BinanceTradeOrderStatus.PARTIALLY_FILLED
                            else:
                                errorMsg = f"Unable to update the order due to invalid status eventType - {message['eventType']}  status : {message['status']} currentStatus {orderDetail.status}"
                                raise Exception(errorMsg)
                            logging.info(f"orderDetail status {orderDetail.status} clientid {orderDetail.clientid} amount {orderDetail.amount} freezed {orderDetail.freezed}")
                            executions = []
                            if orderDetail.trandata is not None:
                                executions = json.loads(orderDetail.trandata)['executions']

                            processTrades(orderId=message['orderId'],orderDetail=orderDetail,executions=executions,trades=trades,db_session=db_session)
                            orderDetail.exchgorderid = message['orderId']
                            orderDetail.trandata= json.dumps({"executions": executions})
                            orderDetail.status = status
                            orderDetail.updatedtime = datetime.datetime.now()  
                            db_session.flush()
                            db_session.commit() 
                            
                        if message['status'] == 'FILLED':
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")                    
                            trades = json.loads(json.dumps(message['fills']))
                            trades = [{**trade, 'isMaker' : False} if 'isMaker' not in trade else trade for trade in trades]
                            if orderDetail.status in [BinanceTradeOrderStatus.NEW,BinanceTradeOrderStatus.ORDER_PLACED,BinanceTradeOrderStatus.PARTIALLY_FILLED]:
                                status= BinanceTradeOrderStatus.FULLY_FILLED
                            else:
                                errorMsg = f"Unable to update the order due to invalid status eventType - {message['eventType']} status : {message['status']} currentStatus {orderDetail.status}"
                                raise Exception(errorMsg)
                            logging.info(f"orderDetail status {orderDetail.status} clientid {orderDetail.clientid} amount {orderDetail.amount} freezed {orderDetail.freezed}")
                            executions = []
                            if orderDetail.trandata is not None:
                                executions = json.loads(orderDetail.trandata)['executions']
                                
                            processTrades(orderId=message['orderId'],orderDetail=orderDetail,executions=executions,trades=trades,db_session=db_session)
                            orderDetail.exchgorderid = message['orderId']
                            orderDetail.trandata= json.dumps({"executions": executions})
                            orderDetail.status = status
                            orderDetail.action = BinanceTradeAction.TO_CLOSE
                            orderDetail.updatedtime = datetime.datetime.now() 
                            db_session.flush()
                            db_session.commit()
                                
                        if message['status'] == "CANCELED":
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")
                            logging.info(f"orderDetail status {orderDetail.status} clientid {orderDetail.clientid} amount {orderDetail.amount} freezed {orderDetail.freezed}")
                            status = None
                            if orderDetail.status == BinanceTradeOrderStatus.ORDER_PLACED or orderDetail.status == BinanceTradeOrderStatus.CANCEL_OPEN_ORDERS:
                                status = BinanceTradeOrderStatus.ORDER_PLACED_AND_CANCELLED
                            elif orderDetail.status == BinanceTradeOrderStatus.PARTIALLY_FILLED:
                                status = BinanceTradeOrderStatus.PARTIALLY_FILLED_AND_CANCELLED
                            else:
                                errorMsg = f"Unable to update the order due to invalid status eventType - {message['eventType']}  status : {message['status']} currentStatus {orderDetail.status}"
                                raise Exception(errorMsg)
                            orderDetail.status = status
                            orderDetail.action = BinanceTradeAction.TO_CLOSE
                            orderDetail.updatedtime = datetime.datetime.now()
                            db_session.flush()
                            db_session.commit() 
                                                
                        if message['status'] == "REJECTED":
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")
                            orderDetail.status= BinanceTradeOrderStatus.REJECTED,
                            orderDetail.updatedtime = datetime.datetime.now()
                            if message['ordertype'] == 0:
                                msg = str(p_msg['BORMREJECT']).format(str('%.8f' % currencytrim(float(message['amount']), coinpairdata[message['coinpair']]["TZQ"])))
                            else:
                                msg = str(p_msg['SORLREJECT']).format(
                                    str('%.8f' % currencytrim(float(message['amount_g']), coinpairdata[message['coinpair']]["TZQ"])) + " " + str(message['coinpair']),
                                    str('%.8f' % float(message['price'])) + " " + str(message['basecoin']))
                            db_session.add(p_notify.PNOTIFY(msg=msg, ctid=message['clientid'], flag=1, type=1))
                            db_session.flush()
                            db_session.commit() 
                            
                        if message['status'] == "INTERNAL_REJECT":
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")
                            orderDetail.status = BinanceTradeOrderStatus.INTERNAL_REJECT
                            orderDetail.action = BinanceTradeAction.TO_CLOSE
                            orderDetail.updatedtime = datetime.datetime.now()
                            if message['ordertype'] == 0:
                                msg = str(p_msg['BORMREJECT']).format(str('%.8f' % currencytrim(float(message['amount']), coinpairdata[message['coinpair']]["TZQ"])))
                            else:
                                msg = str(p_msg['SORLREJECT']).format(
                                    str('%.8f' % currencytrim(float(message['amount_g']), coinpairdata[message['coinpair']]["TZQ"])) + " " + str(message['coinpair']),
                                    str('%.8f' % float(message['price'])) + " " + str(message['basecoin']))
                            db_session.add(p_notify.PNOTIFY(msg=msg, ctid=message['clientid'], flag=1, type=1))
                            db_session.flush()
                            db_session.commit()
                            
                        if message['status'] == "FILTERS_FAILURE":
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")
                            orderDetail.status = BinanceTradeOrderStatus.FILTERS_FAILURE
                            orderDetail.action = BinanceTradeAction.TO_CLOSE
                            orderDetail.updatedtime = datetime.datetime.now()
                            if message['ordertype'] == 0:
                                msg = str(p_msg['BORMREJECT']).format(str('%.8f' % currencytrim(float(message['amount']), coinpairdata[message['coinpair']]["TZQ"])))
                            else:
                                msg = str(p_msg['SORLREJECT']).format(
                                    str('%.8f' % currencytrim(float(message['amount_g']), coinpairdata[message['coinpair']]["TZQ"])) + " " + str(message['coinpair']),
                                    str('%.8f' % float(message['price'])) + " " + str(message['basecoin']))
                            db_session.add(p_notify.PNOTIFY(msg=msg, ctid=message['clientid'], flag=1, type=1))
                            db_session.flush()
                            db_session.commit() 
                            
                        if message['status'] == 'EXPIRED':
                            logging.info(f"Consumer_process_event eventType - {message['eventType']} - eventName : {message['eventName']} - data -> {message}")
                            logging.info(f"orderDetail status {orderDetail.status} clientid {orderDetail.clientid} amount {orderDetail.amount} freezed {orderDetail.freezed}")
                            status = None
                            if orderDetail.status == BinanceTradeOrderStatus.ORDER_PLACED:
                                status = BinanceTradeOrderStatus.ORDER_PLACED_AND_EXPIRED
                            elif orderDetail.status == BinanceTradeOrderStatus.PARTIALLY_FILLED:
                                status = BinanceTradeOrderStatus.PARTIALLY_FILLED_AND_EXPIRED
                            else:
                                errorMsg = f"Unable to update the order due to invalid status eventType - {message['eventType']}  status : {message['status']} currentStatus {orderDetail.status}"
                                raise Exception(errorMsg)
                            orderDetail.status= status
                            orderDetail.action = BinanceTradeAction.TO_CLOSE
                            orderDetail.updatedtime = datetime.datetime.now()
                            db_session.flush()
                            db_session.commit() 
                            
                    except Exception as e:
                        msg = errorMsg if errorMsg else str(e)
                        logging.exception(f"Unable to process event for order {clientOrderId} due to {msg}")
                        db_session.rollback()
                    finally:
                        if db_session:
                            db_session.close()
            time.sleep(serviceMgmt.serviceConfig['binanceconfig']['processEventConsumerSleepInSeconds'])
    except Exception as e:
        logging.exception(f"Unable to process event  consumer dur to {str(e)}")
        

def processTrades(orderId=None,orderDetail=None,executions=None,trades=None,db_session=None):
    ctwalletbase =getCtWalletObject(ctid=orderDetail.clientid,curr=orderDetail.basecoin,dbsession=db_session)
    ctwalletpair = getCtWalletObject(ctid=orderDetail.clientid,curr=orderDetail.paircoin,dbsession=db_session)
    gwalletbase = getGioWalletObject(gtype=6, curr=orderDetail.basecoin,db_session=db_session)
    gwalletpair = getGioWalletObject(gtype=6, curr=orderDetail.paircoin,db_session=db_session)
    exwalletbase = getExWalletObject(exchangeid=1, coin=orderDetail.basecoin,db_session=db_session)
    exwalletpair = getExWalletObject(exchangeid=1, coin=orderDetail.paircoin,db_session=db_session)
    for transaction in trades:
        if len(executions) == 0 or (not any(transaction['tradeId'] == executedData['tradeId'] for executedData in executions)):
            executions.append(transaction)
            amount,freezed = processBinanceOrderTransaction(order=orderDetail,data=transaction,ctwalletbase=ctwalletbase,ctwalletpair=ctwalletpair,gwalletbase=gwalletbase,gwalletpair=gwalletpair,exwalletbase=exwalletbase,exwalletpair=exwalletpair,db_session=db_session)
            logging.info(f"orderID - {orderDetail.id} amount - {amount} freezed - {freezed}")
            logging.info(f"CT Wallet after all deduction : ctwalletpair.value - {ctwalletpair.value} | ctwalletpair.freezed - {ctwalletpair.freezed} | ctwalletbase.value {ctwalletbase.value} | ctwalletbase.freezed - {ctwalletbase.freezed} ") 
            logging.info(f"GIO Wallet after all deduction : gwalletpair.value - {gwalletpair.value} | gwalletbase.value - {gwalletbase.value}")
            logging.info(f"EXCHG Wallet after all deduction : exwalletpair.value - {exwalletpair.value} | exwalletpair.freezed - {exwalletpair.freezed} | exwalletbase.value - {exwalletbase.value} | exwalletbase.freezed - {exwalletbase.freezed}")

def processBinanceOrderTransaction(order=None,data=None,ctwalletbase=None,ctwalletpair=None,gwalletbase=None,gwalletpair=None,exwalletbase=None,exwalletpair=None,isMaker = False,db_session=None):
    logging.info(str(data))
    maker = 1
    tradeFeesP = "0"
    executed_quantity = truncate_decimal(Decimal(data['qty']),coinpairdata[order.coinpair]["TZQ"])
    executed_price = truncate_decimal(Decimal(data['price']),coinpairdata[order.coinpair]["TZP"])
    #reduced qty
    order_quantity_a =truncate_decimal(Decimal(str(order.amount)), coinpairdata[order.coinpair]["TZQ"])
    order.amount = truncate_decimal(order_quantity_a - executed_quantity,coinpairdata[order.coinpair]["TZQ"])
    
    order_quantity = truncate_decimal(Decimal(str(order.amount)),coinpairdata[order.coinpair]["TZQ"])
    order_price = truncate_decimal(Decimal(str(order.price)),coinpairdata[order.coinpair]["TZP"])
    total_quantity = truncate_decimal(Decimal(str(order.amount_g)),coinpairdata[order.coinpair]["TZQ"])

    executed_total = truncate_decimal(executed_quantity * executed_price ,coinpairdata[order.coinpair]["TZP"])
    order_total = truncate_decimal(executed_quantity * order_price ,coinpairdata[order.coinpair]["TZP"])
    
    dfreeze = order_total - executed_total # get the difference
    logging.info(f"wallet dfreezing value : {dfreeze}")
        
    tdsA = 0
    tdsB = 0
    if data['isMaker']:
        feeb = executed_quantity * coinpairdata[order.coinpair]["fees"]["MB"]
        fees = executed_total * coinpairdata[order.coinpair]["fees"]["MS"]
    else:
        feeb = executed_quantity * coinpairdata[order.coinpair]["fees"]["TB"]
        fees = executed_total * coinpairdata[order.coinpair]["fees"]["TS"]
    # logging.info(f"Default fees(coinpair_price): feeb:{feeb} fees:{fees}")
    # logging.info(f"Initial TDS: tdsA:{tdsA} tdsB:{tdsB}")
    if serviceMgmt.serviceInfo['breakers']['use_slab_based_fee']:
        try:
            tradeFees = requests.get(
                url=serviceMgmt.appInstanceUrlMap.get("trading_fee_service") 
                + "/trade/fees?ctid={ctid}&coinPair={coinPair}".format(ctid=order.clientid, coinPair=order.coinpair)).text
            logging.info(f"Response from trading fee service : {tradeFees}")
            tradeFees = json.loads(tradeFees)["Data"]["fees"]
            tradeFeesP = tradeFees[("M" if data['isMaker'] else "T") + ortype[int(order.ordertype)]]
            if data['isMaker']:
                feeb = executed_quantity * Decimal(tradeFees["MB"])
                fees = executed_total * Decimal(tradeFees["MS"])
            else:
                feeb = executed_quantity * Decimal(tradeFees["TB"])
                fees = executed_total * Decimal(tradeFees["TS"])
            # logging.info(f"Overrided fees from fee service: feeb:{feeb} fees:{fees}")
        except:
            logging.exception("")
            raise Exception("Unable to retrieve fee data")
    
    # logging.info(f"Final fees: feeb:{feeb} fees:{fees}")
    if int(order.ordertype) == 1:
        logging.info("Binance Sell Order")
        fee = [fees, feeb]
        
        if int(order.clientid) != tds_admin:
            tdsA = Decimal(str(order.tds)) * (executed_total - fees)
        logging.info(f"After deducting TDS, TDS value: {tdsA}")

        # ct wallet    
        ctwalletpair.freezed = truncate_decimal(Decimal(str(ctwalletpair.freezed)) - executed_quantity, coinpairdata[order.coinpair]["TZQ"])
        ctwalletbase.value = Decimal(str(ctwalletbase.value)) + (executed_total - fees - tdsA)
        
        #gio wallet 
        gwalletbase.value = Decimal(str(gwalletbase.value)) + fees
        
        # exchange wallet
        exwalletbase.value = truncate_decimal(Decimal(str(exwalletbase.value)) + executed_total - Decimal(str(data['commission'])), coinpairdata[order.coinpair]["TZP"])
        exwalletpair.freezed = truncate_decimal(Decimal(str(exwalletpair.freezed)) - executed_quantity, coinpairdata[order.coinpair]["TZQ"])
        
        order.freezed = truncate_decimal(Decimal(str(order.freezed)) - executed_quantity, coinpairdata[order.coinpair]["TZQ"])
        logging.info(f"Wallet Details  ctwalletpair.freezed - {ctwalletpair.freezed} ctwalletbase.value - {ctwalletbase.value} gwalletbase.value - {gwalletbase.value} exwalletpair.freezed - {exwalletpair.freezed} exwalletpair.value - {exwalletpair.value} exwalletbase.value - {exwalletbase.value}")
        Wallet.addHistory(type=8, value=executed_total, trantype=0, wallet=ctwalletbase,session=db_session, fee=fees)
        tdsdataA = {"txid": "txid", "orderid": order.id, "producttype": 2, "ctid": order.clientid,"coin": order.basecoin,"quantity":tdsA}    
            
    elif int(order.ordertype) == 0:
        logging.info("Binance Buy Order")
        fee = [feeb,fees]
        
        if int(order.clientid) != tds_admin:
            tdsA = Decimal(str(order.tds)) * (executed_quantity - feeb)
        # logging.info(f"After deducting TDS, TDS value: {tdsA}")
        # logging.info(f"executed_quantity : {executed_quantity}  executed_price: {executed_price}  executed_total: {executed_total}")
        # logging.info(f"order_quantity : {executed_quantity}  order_price: {order_price}  order_total: {order_total}")
        # logging.info(f"dfreeze(order_tot-exec_tot) {dfreeze}")
        # ct wallet
        ctwalletpair.value = Decimal(str(ctwalletpair.value)) + (executed_quantity - feeb - tdsA)
        # logging.info(f'C)Checking ctwallet.freeze {ctwalletbase.freezed}')
        ctwalletbase.freezed = truncate_decimal(Decimal(str(ctwalletbase.freezed)) - (executed_total + dfreeze), coinpairdata[order.coinpair]["TZP"])
        # logging.info(f'D)Checking ctwallet.freeze {ctwalletbase.freezed}')
        ctwalletbase.value = truncate_decimal(Decimal(str(ctwalletbase.value)) + dfreeze, coinpairdata[order.coinpair]["TZP"])
        gwalletpair.value = Decimal(str(gwalletpair.value)) + feeb
        
        # exchange wallet
        exwalletpair.value = truncate_decimal(Decimal(str(exwalletpair.value)) + executed_quantity - Decimal(str(data['commission'])),coinpairdata[order.coinpair]["TZQ"])
        exwalletbase.freezed = truncate_decimal(Decimal(str(exwalletbase.freezed)) - (executed_total + dfreeze), coinpairdata[order.coinpair]["TZP"])
        exwalletbase.value = truncate_decimal(Decimal(str(exwalletbase.value)) + dfreeze, coinpairdata[order.coinpair]["TZP"])
        # logging.info(f"A)Checking order.freeze {str(order.freezed)}")
        order.freezed = truncate_decimal(Decimal(str(order.freezed)) - (executed_total + dfreeze), coinpairdata[order.coinpair]["TZP"])
        # logging.info(f"B)Checking order.freeze {str(order.freezed)}")

        Wallet.addHistory(type=7, value=executed_total, trantype=0, wallet=ctwalletpair,session=db_session, fee=feeb)
        logging.info(f"Wallet details : ctwalletpair.value {ctwalletpair.value} - gwalletpair.value - {gwalletpair.value} exwalletpair.value - {exwalletpair.value} ctwalletbase.freezed {ctwalletbase.freezed} exwalletbase.value - {exwalletbase.value}")
        tdsdataA = {"txid": "txid", "orderid": order.id, "producttype": 1, "ctid": order.clientid,"coin": order.paircoin,"quantity":tdsA}
    logging.info(f"Processing Binance Order : {order.id} executed_quantity - {executed_quantity} executed_price - {executed_price}  order_quantity - {order_quantity} order_price - {order_price} executed_total - {executed_total} order_total - {order_total} total_quantity - {total_quantity}")
    transacid = unpack("!Q", urandom(8))[0]

    newtran = order_transaction.ORDERTRANSACTION(
        orderidA=str(order.id),
        transacid=transacid,
        orderidB=str(0),
        orderidsecA=str(0),
        orderidsecB=str(0),
        ordertypeA=str(order.ordertype),
        ordertypeB=str(0),
        trantypeA=str(order.trantype),
        trantypeB=str(0),
        orderApair=str(order.coinpair),
        orderBpair=str(order.coinpair),
        price=str(executed_price),
        quantity=str(executed_quantity),
        total=str(executed_total),
        userA=str(order.clientid),
        userB=str(0),
        feesA=str(fee[0]),
        feesB='0',
        exchid=str(serviceMgmt.serviceConfig['binanceconfig']['exchg_id']), maker=str(maker), tdsA=str(tdsA), tdsB=str(tdsB),exchoid=order.exchgorderid,exchtxid="",
        priceT=str(0),
        quantityT=str(0),
        totalT=str(0),
        exectype=0,
        feesAp=str(tradeFeesP),
        feesBp=str(0),
        tdsAp=str(order.tds),
        tdsBp=str(0),
        tradeorderidA=int(order.tradeorderid)
    )

    db_session.add(newtran)
    db_session.flush()
    try:
        msg = None
        if order.amount == 0:
            msg = str(p_msg[ortype[int(order.ordertype)] + 'ORSEXE']).format(str('%.8f' % currencytrim(float(order.amount_g), coinpairdata[order.coinpair]["TZQ"])))
        else:
            msg = str(p_msg[ortype[int(order.ordertype)] + 'ORPEXE']).format(
                str('%.8f' % currencytrim(float(order.amount_g), coinpairdata[order.coinpair]["TZQ"])) + " " + str(
                    order.paircoin),
                str('%.8f' % executed_price) + " " + str(order.basecoin),
                str('%.8f' % currencytrim(float(executed_quantity), coinpairdata[order.coinpair]["TZQ"])) + " " + str(order.paircoin))
        db_session.add(p_notify.PNOTIFY(msg=msg, ctid=order.clientid, flag=1, type=1))
    except Exception as e:
        logging.exception(f"Unable to push notification due to {str(e)}")
    if float(str(tdsA)) > 0:
        tdsdataA["txid"] = newtran.id
        tdsAobj = getCtWalletObject(ctid=tds_admin, curr=tdsdataA["coin"], dbsession=db_session)
        tdsAobj.value = Decimal(tdsAobj.value) + Decimal(tdsA)
        createtdsorder(giodbsession=db_session, tdsdata=tdsdataA, tds_config=tds_config)
    logging.info(f"Processing Binance Order order.amount {order.amount} order.freezed {order.freezed}")
    return order.amount,order.freezed

    
def currencytrim(value,dplaces):
    return int(float(value)*pow(10,dplaces))/(pow(10,dplaces)*1.0)


#Job to process the order when the order got fully executed or canceled               
"""
get the orders with action to_close
make an api call to get the transaction data
validate the transactions data
if it is correct set the action to closed
if not update the correct transaction data and set the action to closed
"""

def validateOrder():
    from DataController3.utils import session_maker
    db_session = scoped_session(session_maker)
    while True:
        logging.info(f"Process validate order started - Breakers process_validate_orders {get_service_info()['breakers']['process_validate_orders']}")
        if get_service_info()["breakers"]["process_validate_orders"]:
            try:    
                clientorderids = db_session.query(label("clientorderids", func.group_concat(BINANCETRADEORDERS.clientorderid))).filter(BINANCETRADEORDERS.action == BinanceTradeAction.TO_CLOSE).all()
                if clientorderids[0][0] is not None:
                    clientorderids = clientorderids[0][0].split(",")
                    logging.info(f"Validate Order -> {clientorderids}")
                    trandata = None
                    apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
                    for clientorderid in clientorderids:
                        try:
                            order = db_session.query(BINANCETRADEORDERS).filter(BINANCETRADEORDERS.clientorderid == clientorderid).with_for_update().one() # use withforupdate
                            logging.info(f"Order details {order}")
                            ctwalletbase = getCtWalletObject(ctid = order.clientid, curr = order.basecoin, dbsession = db_session)
                            ctwalletpair = getCtWalletObject(ctid = order.clientid, curr = order.paircoin, dbsession = db_session)
                            gwalletbase = getGioWalletObject(gtype=6, curr=order.basecoin,db_session=db_session)
                            gwalletpair = getGioWalletObject(gtype=6, curr=order.paircoin,db_session=db_session)
                            exwalletbase = getExWalletObject(exchangeid=1, coin=order.basecoin,db_session=db_session)
                            exwalletpair = getExWalletObject(exchangeid=1, coin=order.paircoin,db_session=db_session)
                            if order.status in [BinanceTradeOrderStatus.ORDER_PLACED_AND_CANCELLED,BinanceTradeOrderStatus.ORDER_PLACED_AND_EXPIRED,BinanceTradeOrderStatus.REJECTED,BinanceTradeOrderStatus.INTERNAL_REJECT,BinanceTradeOrderStatus.FILTERS_FAILURE]:
                                if order.ordertype == 0:
                                    ctwalletbase.freezed = truncate_decimal(str(ctwalletbase.freezed)) - truncate_decimal(Decimal(str(order.freezed)))
                                    ctwalletbase.value = truncate_decimal(str(ctwalletbase.value)) + truncate_decimal(Decimal(str(order.freezed)))
                                    exwalletbase.freezed = truncate_decimal(str(exwalletbase.freezed)) - truncate_decimal(Decimal(str(order.freezed)))
                                    exwalletbase.value = truncate_decimal(str(exwalletbase.value)) + truncate_decimal(Decimal(str(order.freezed)))
                                else:
                                    ctwalletpair.freezed = truncate_decimal(str(ctwalletpair.freezed)) - truncate_decimal(Decimal(str(order.freezed)))
                                    ctwalletpair.value = truncate_decimal(str(ctwalletpair.value)) + truncate_decimal(Decimal(str(order.freezed)))
                                    exwalletpair.freezed = truncate_decimal(str(exwalletpair.freezed)) - truncate_decimal(Decimal(str(order.freezed)))
                                    exwalletpair.value = truncate_decimal(str(exwalletpair.value)) + truncate_decimal(Decimal(str(order.freezed)))
                                order.action = BinanceTradeAction.CLOSED
                                db_session.flush()
                                db_session.commit()
                            elif order.status in [BinanceTradeOrderStatus.PARTIALLY_FILLED_AND_CANCELLED,BinanceTradeOrderStatus.PARTIALLY_FILLED_AND_EXPIRED]:
                                binanceTradeDetail = apiClient.getTradesByOrder(symbol = coinpairdata[order.coinpair]['EXGDT1']['symbol'],orderId = order.exchgorderid)
                                binance_trandata = []
                                amount = Decimal(str(order.amount_g))
                                freezed = truncate_decimal(Decimal(str(order.amount_g)) * Decimal(str(order.price)),coinpairdata[order.coinpair]["TZP"]) if order.ordertype == 0 else Decimal(str(order.amount_g))
                                for trandata in binanceTradeDetail:
                                    new_trandata = {
                                                    "price":trandata['price'],
                                                    "qty":trandata['qty'],
                                                    "commission":trandata['commission'],
                                                    "commissionAsset":trandata['commissionAsset'],
                                                    "tradeId":trandata['id'],
                                                    "isMaker" : trandata['isMaker']
                                                }
                                    if order.ordertype == 0:
                                        amount = truncate_decimal(amount - Decimal(trandata['qty']),coinpairdata[order.coinpair]["TZQ"])
                                        freezed = truncate_decimal(freezed - (Decimal(str(order.price)) * Decimal(trandata['qty'])),coinpairdata[order.coinpair]["TZP"])
                                    else:
                                        amount = truncate_decimal(amount - Decimal(str(trandata['qty'])),coinpairdata[order.coinpair]["TZQ"])
                                        freezed = truncate_decimal(freezed - Decimal(str(trandata['qty'])),coinpairdata[order.coinpair]["TZQ"])
                                    binance_trandata.append(new_trandata)
                                    
                                if order.trandata is not None:
                                    trandata = json.loads(order.trandata)
                                    trandata = trandata['executions']
                                logging.info(f"binance_trandata - {binance_trandata}")
                                logging.info(f"trandata - {trandata}")
                                total_executed_value = 0
                                for transaction in binance_trandata:
                                    if not any(transaction['tradeId'] == tradeData['tradeId'] for tradeData in trandata):
                                        process_amount,process_freezed = processBinanceOrderTransaction(order=order,data=transaction,ctwalletbase=ctwalletbase,ctwalletpair=ctwalletpair,gwalletbase=gwalletbase,gwalletpair=gwalletpair,exwalletbase=exwalletbase,exwalletpair=exwalletpair,db_session=db_session)
                                        logging.info(f"Processed missing transactions for order {order.id} transaction {transaction}  processed_amount {process_amount} {process_freezed}")
                                    if order.ordertype == 0:
                                        total_executed_value = total_executed_value + truncate_decimal((Decimal(transaction['qty']) * Decimal(transaction['price'])),coinpairdata[order.coinpair]["TZP"])
                                    else:
                                        total_executed_value = truncate_decimal(total_executed_value + Decimal(transaction['qty']),coinpairdata[order.coinpair]["TZQ"])
                                # unfreezing the freezed values in ct_wallet and exchange_wallet
                                if order.ordertype == 0:
                                    remaining_freezed = truncate_decimal((Decimal(str(order.amount_g)) * Decimal(str(order.price))),coinpairdata[order.coinpair]["TZP"]) - total_executed_value                                
                                    ctwalletbase.value = truncate_decimal(str(ctwalletbase.value)) + truncate_decimal(remaining_freezed)
                                    ctwalletbase.freezed = truncate_decimal(str(ctwalletbase.freezed)) - truncate_decimal(remaining_freezed)
                                    exwalletbase.value = truncate_decimal(str(exwalletbase.value)) + truncate_decimal(remaining_freezed)
                                    exwalletbase.freezed = truncate_decimal(str(exwalletbase.freezed)) - truncate_decimal(remaining_freezed)
                                else:
                                    remaining_freezed =  truncate_decimal(Decimal(str(order.amount_g)) - total_executed_value,coinpairdata[order.coinpair]["TZQ"])
                                    ctwalletpair.value = truncate_decimal(str(ctwalletpair.value)) + truncate_decimal(remaining_freezed)
                                    ctwalletpair.freezed = truncate_decimal(str(ctwalletpair.freezed)) - truncate_decimal(remaining_freezed)
                                    exwalletpair.value = truncate_decimal(str(exwalletpair.value)) + truncate_decimal(remaining_freezed)
                                    exwalletpair.freezed = truncate_decimal(str(exwalletpair.freezed)) - truncate_decimal(remaining_freezed)
                                order.trandata = json.dumps({"executions":binance_trandata})
                                transferTruncationDust(freezed=freezed, order= order, ctwalletbase=ctwalletbase, exwalletbase=exwalletbase, db_session=db_session)
                                order.action = BinanceTradeAction.CLOSED
                                order.amount = amount
                                db_session.flush()
                                db_session.commit()
                                    
                            elif order.status  == BinanceTradeOrderStatus.FULLY_FILLED:
                                binanceTradeDetail = apiClient.getTradesByOrder(symbol = coinpairdata[order.coinpair]['EXGDT1']['symbol'],orderId = order.exchgorderid)
                                logging.info(f"{binanceTradeDetail}");
                                binance_trandata = []
                                amount = Decimal(str(order.amount_g))
                                freezed = truncate_decimal(Decimal(str(order.amount_g)) * Decimal(str(order.price)),coinpairdata[order.coinpair]["TZP"]) if order.ordertype == 0 else Decimal(str(order.amount_g))
                                for trandata in binanceTradeDetail:
                                    new_trandata = {
                                                    "price":trandata['price'],
                                                    "qty":trandata['qty'],
                                                    "commission":trandata['commission'],
                                                    "commissionAsset":trandata['commissionAsset'],
                                                    "tradeId":trandata['id'],
                                                    "isMaker" : trandata['isMaker']
                                                }
                                    if order.ordertype == 0:
                                        amount = truncate_decimal(amount - Decimal(trandata['qty']),coinpairdata[order.coinpair]["TZQ"])
                                        freezed = freezed - truncate_decimal((Decimal(str(order.price)) * Decimal(trandata['qty'])),coinpairdata[order.coinpair]["TZP"])
                                    else:
                                        amount = truncate_decimal(amount - Decimal(trandata['qty']),coinpairdata[order.coinpair]["TZQ"])
                                        freezed = truncate_decimal(freezed - Decimal(trandata['qty']),coinpairdata[order.coinpair]["TZQ"])
                                    binance_trandata.append(new_trandata)
                                
                                logging.info(f"{binance_trandata}")
                                if order.trandata is not None:
                                    trandata = json.loads(order.trandata)
                                    trandata = trandata['executions']
                                logging.info(f"binance_trandata - {binance_trandata}")
                                logging.info(f"trandata - {trandata}")
                                for transaction in binance_trandata:
                                    if not any(transaction['tradeId'] == tradeData['tradeId'] for tradeData in trandata):
                                        process_amount,process_freezed = processBinanceOrderTransaction(order=order,data=transaction,ctwalletbase=ctwalletbase,ctwalletpair=ctwalletpair,gwalletbase=gwalletbase,gwalletpair=gwalletpair,exwalletbase=exwalletbase,exwalletpair=exwalletpair,db_session=db_session)
                                        logging.info(f"Processed missing transactions for order {order.id} transaction {transaction}  processed_amount {process_amount} {process_freezed}")
                                order.trandata = json.dumps({"executions":binance_trandata})
                                transferTruncationDust(freezed=freezed, order= order, ctwalletbase=ctwalletbase, exwalletbase=exwalletbase, db_session=db_session)
                                order.action = BinanceTradeAction.CLOSED
                                order.amount = amount
                                db_session.flush()
                                db_session.commit()    
                        except Exception as e:
                            logging.exception(f"Unable to validate the order {order.id} due to {str(e)}") 
                            db_session.rollback() 
                        finally:
                            if db_session:
                                db_session.close()      
            except Exception as e:
                logging.exception(f"{str(e)}")
        time.sleep(serviceMgmt.serviceConfig["binanceconfig"]['process_validate_orders_in_secs'])



"""
Pick the stale order with action create
check the last updated time if its if more than 2 mins
make an api call and get the trandata
and push the response to event queue
"""
def processStaleOrders():
    from DataController3.utils import session_maker
    db_session = scoped_session(session_maker)
    while True:
        logging.info(f"Process stale order started - Breakers process_stale_orders {get_service_info()['breakers']['process_stale_orders']}")
        if get_service_info()["breakers"]["process_stale_orders"]:
            clientorderids = db_session.query(label("clientorderids", func.group_concat(BINANCETRADEORDERS.clientorderid))).filter(BINANCETRADEORDERS.action == BinanceTradeAction.CREATE).all()
            logging.info(f"Stale Orders -> {clientorderids}")
            apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])
            if clientorderids[0][0] is not None:
                clientorderids = clientorderids[0][0].split(",")
                for clientorderid in clientorderids:
                    logging.info(f"ClientId -> {clientorderid}")
                    orderID = None
                    try:
                        order = db_session.query(BINANCETRADEORDERS).filter(BINANCETRADEORDERS.clientorderid == clientorderid).with_for_update().one()
                        lastupdatedTime = datetime.datetime.strptime(str(order.updatedtime),'%Y-%m-%d %H:%M:%S')
                        currenctTime = datetime.datetime.now()
                        diff = currenctTime - lastupdatedTime
                        diff_in_minutes = diff.total_seconds()
                        orderData = {
                            "clientid" : order.clientid,
                            "clientOrderId" : str(order.clientorderid),
                            "price" : order.price, 
                            "amount" : order.amount, 
                            "amount_g" : order.amount_g,
                            "freezed" : order.freezed,
                            "status" : order.status, 
                            "ordertype" : order.ordertype,
                            "trantype" :  order.trantype,
                            "coinpair" : str(order.coinpair),
                            "basecoin" : str(order.basecoin),
                            "paircoin" : str(order.paircoin), 
                            "exchgid" : serviceMgmt.serviceConfig['binanceconfig']['exchg_id'],
                            "action" : BinanceTradeAction.CREATE,
                        }
                        logging.info(f"___________++++++++ {diff_in_minutes}")
                        if orderData['status'] == BinanceTradeOrderStatus.NEW:
                            logging.info(f"Verified order in status 0 {order}")
                            """
                                When orders are not placed in binance for certain time we will reject the order
                            """
                            if diff_in_minutes > serviceMgmt.serviceConfig['binanceconfig']['order_threshold']: # grater then 10 sec
                                partitionId =int(orderData['clientOrderId']) % orderPartitionCount
                                orderData['eventName'] = "REJECT_ORDER"
                                orderData['status'] = "INTERNAL_REJECT"
                                orderData['orderid'] = order.clientorderid
                                logging.info(f"partition ID for create order -> {partitionId}")
                                binanceProducer.produceMessage(
                                    data=json.dumps(orderData),
                                    topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_topic'],
                                    partition=partitionId
                                )
                                logging.info(f"process_stale_order - eventName : REJECT_ORDER - data -> {orderData}")    
                            else:
                                partitionId =int(order.clientorderid) % orderPartitionCount
                                orderData['eventName'] = "PLACE_ORDER"
                                orderData['orderid'] = order.clientorderid
                                logging.info(f"partition ID for create order -> {partitionId}")
                                binanceProducer.produceMessage(
                                    data=json.dumps(orderData),
                                    topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_topic'],
                                    partition=partitionId
                                )
                                logging.info(f"process_stale_order - eventName : PLACE_ORDER - data -> {orderData}")  
                        # to cancel open order by changing status to 11
                        elif orderData['status'] == BinanceTradeOrderStatus.CANCEL_OPEN_ORDERS:
                            orderData['orderId'] = str(order.exchgorderid)
                            orderData['eventName'] = "CANCEL_ORDER"
                            orderData['eventType'] = "httpEvent"
                            partitionCount = serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_partition']
                            partitionId = int(order.clientorderid) % partitionCount
                            binanceProducer.produceMessage(
                                data=json.dumps(orderData),
                                topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_topic'],
                                partition=partitionId
                            )
                            logging.info("Processing Binance cancel order - " + str(orderData))
                            try:
                                ttype=['M','L','SL','SM']
                                db_session.add(p_notify.PNOTIFY(
                                        msg=str(p_msg[ortype[int(order.ordertype)] + 'OR' + ttype[int(order.trantype)] + 'CANCEL'])
                                        .format(str(order.amount) + " " + str(order.paircoin),str(order.price) + " " + str(order.basecoin)), 
                                        ctid=order.clientid, flag=1,
                                        type=1))
                                db_session.commit()
                            except Exception as e:
                                logging.exception("Unable to Push Notification due to " + str(e))                
                        else:
                            if diff_in_minutes > serviceMgmt.serviceConfig['binanceconfig']['order_details_threshold']: # grater then 2 mins
                                binanceOrderDetail = apiClient.getOrder(symbol = coinpairdata[order.coinpair]['EXGDT1']['symbol'],origClientOrderId = order.clientorderid)
                                binanceTradeDetail = apiClient.getTradesByOrder(symbol = coinpairdata[order.coinpair]['EXGDT1']['symbol'],orderId = order.exchgorderid)
                                logging.info(f"{binanceTradeDetail}");
                                binance_trandata = []
                                for trandata in binanceTradeDetail:
                                    new_trandata = {
                                                    "price":trandata['price'],
                                                    "qty":trandata['qty'],
                                                    "commission":trandata['commission'],
                                                    "commissionAsset":trandata['commissionAsset'],
                                                    "tradeId":trandata['id'],
                                                    "isMaker":trandata['isMaker']
                                                }
                                    binance_trandata.append(new_trandata)
                                if binanceOrderDetail['status'] in ['FILLED',"PARTIALLY_FILLED","CANCELED","EXPIRED"]:
                                    binanceOrderDetail['fills'] = binance_trandata
                                binanceOrderDetail['eventType'] = 'httpEvent'
                                binanceOrderDetail['eventName'] = binanceOrderDetail['status']
                                orderData['orderid'] = order.clientorderid
                                orderID = binanceOrderDetail['orderId']
                                partitionId = int(binanceOrderDetail["orderId"]) % eventPartitionCount
                                binanceProducer.produceMessage(
                                    data=json.dumps(binanceOrderDetail),
                                    topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                                    partition = partitionId,
                                    key = "httpEvent"
                                )
                                logging.info(f"process_stale_order - eventName : get order details - data -> {binanceOrderDetail}")
                    except Exception as e:
                        logging.exception(f"Unable to process the stale order - {orderID} due to {str(e)}")
                        db_session.rollback()
                    finally:
                        if db_session:
                            db_session.close() 
        time.sleep(serviceMgmt.serviceConfig["binanceconfig"]['process_stale_orders_sleep_in_secs'])

def on_message(message):
    logging.info("WebSocket: ",str(message))
        
        
def createUserDataStreamSocket(onmessage=None,apiClient=None):
    logging.info(f"onmessage {onmessage.__name__}")
    try:
        listentoken = apiClient.getStreamListenKey()
        apiClient.closeStream(listenKey=listentoken)
        listentoken = apiClient.getStreamListenKey()
        websocket.enableTrace(True)
        if onmessage is None:
            onmessage = on_message
        #url = "wss://stream.binance.com:9443/stream?streams="+ listentoken
        url = serviceMgmt.serviceConfig["binanceconfig"]['socket_url'] + listentoken
        ws = websocket.WebSocketApp(url,on_message=onmessage)
        logging.info("Socket Created")
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        logging.info( "Socket Closed Automatically" )
    except Exception as e:
        ws.close()
        logging.exception(f"Unable to connect websocket due to {str(e)}")
        
        
def watchBinanceCyptoOrders(*argv):
    try:
        message = json.loads(list(argv)[1])
        message = message['data']
        logging.info(f"Message -> {str(message)}")
        if message['e']=='outboundAccountPosition':
           logging.info(str(["watchCyptoOrders", list(argv)]))
           logging.info(f"Producer websocker -> {message}")
        elif message['e']=='executionReport':
            orderId = message['c']
            if message['X'] == 'CANCELED':
                orderId = message['C']
            logging.info(f"WEsocket Orderid : {orderId}")
            partitionId = int(orderId) % eventPartitionCount
            message['eventType'] = 'wsocketEvent'
            message['eventName'] = message['x']
            data = {
                "orderId" : message['i'],
                "clientOrderId" : orderId,
                "transactTime" :  message['T'],
                "price" :  message['p'],
                "origQty" :  message['q'],
                "executedQty" :  message['z'],
                "cummulativeQuoteQty" : message['Z'],
                "status" :  message['X'],
                "timeInForce" :  message['f'],
                "type" :  message['o'],
                "side" :  message['S'],
                "fills":[
                    {
                        "price": message['L'],
                        "qty": message['l'],
                        "commission": message['n'],
                        "commissionAsset": message['N'],
                        "tradeId": message['t'],
                        "isMaker" : message['m']
                    }
                ],
                "eventType" : "wsocketEvent",
                "eventName" : message['x']
            }
            logging.info(f"watchBinanceCyptoOrderstype - {message}")
            logging.info(f'PartitionId wsevent: {partitionId}  Orderid : {orderId}')
            binanceProducer.produceMessage(
                data=json.dumps(data),
                topic=serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_topic'],
                partition=partitionId,
                key="wsocketEvent"
            )    
    except Exception as e:
        logging.exception(f"Unable to process the websocket event due to {str(e)}")
        
def get_service_info():
    return serviceMgmt.serviceInfo       

def set_orderbook_snapshot(symbol):
    global orderbook, orderBookCoinPairEventQueue, exchangecoinpairdata, orderbookSnapshotStatus

    try:
        if symbol in orderbookSnapshotStatus:
            orderbookSnapshotStatus[symbol] = False

        if symbol not in exchangecoinpairdata:
            return

        response = requests.get(serviceMgmt.serviceConfig['binanceconfig']["orderbook"]['snap_uri'].format(symb=symbol))
        snapshots = response.json()
        # logging.info(f"orderbook=====>{orderbook}, snapshots====>{snapshots}")
        if('code' not in snapshots and 'bids' in snapshots and 'asks' in snapshots):
            bidsTmp = {}
            asksTmp = {}
            for row in snapshots["bids"]:
                bidsTmp[Decimal(row[0])] = row

            for row in snapshots["asks"]:
                asksTmp[Decimal(row[0])] = row

            orderbook[symbol]['lastUpdateId'] = int(snapshots['lastUpdateId'])
            orderbook[symbol]["bids"] = bidsTmp
            orderbook[symbol]["asks"] = asksTmp
            orderbookSnapshotStatus[symbol] = True

            coinPair = exchangecoinpairdata[symbol]['pairname']
            orderBookCoinPairEventQueue[coinPair] = []
            logging.info(f"orderbook for {symbol} refreshed using snapshot")
        elif 'msg' in snapshots:
            logging.info(f"snapshot for {symbol} is not updated due to {snapshots['msg']}")
        else:
            logging.info(f"Required details are missing.")
    except Exception as e:
        logging.exception("set_orderbook_snapshot")
        logging.exception(json.dumps({'Status': 'Error', 'Msg': str(e)}))

def refresh_orderbook_snapshot():
    global refreshOrderbookSnapshotBatch
    
    while True:
        if not get_service_info()["breakers"]["refresh_orderbook_snapshot"]:
            logging.info(f"Refresh orderbook snapshot - Breakers refresh orderbook snapshot {get_service_info()['breakers']['refresh_orderbook_snapshot']}")
            return
        
        count = 0
        for symbol in dict(refreshOrderbookSnapshotBatch):
            set_orderbook_snapshot(symbol)
            count += 1
            thread_lock.acquire()
            try:
                refreshOrderbookSnapshotBatch.pop(symbol, None)
            finally:
                thread_lock.release()
            if(count >= serviceMgmt.serviceConfig['binanceconfig']['refreshOrderBookSnapshotBatchSize']):
                time.sleep(serviceMgmt.serviceConfig['binanceconfig']['refreshOrderBookSnapshotSleepInSeconds'])
                count = 0

def updateOrderBookData(type,incData,symbol):
    global orderbook
    # updating our orders in  orderbook with live incremental data
    for order in incData:
        if Decimal(order[0]) in orderbook[symbol][type]:
            if (Decimal(order[1]) == 0.0):
                # deleting this row since qty is 0
                del orderbook[symbol][type][Decimal(order[0])]
            else:
                orderbook[symbol][type][Decimal(order[0])] = order
        elif (Decimal(order[1]) != 0.0):
            orderbook[symbol][type][Decimal(order[0])] = order

def process_orderbook_events(coinSymbol, coinPair):
    global orderbook, orderBookCoinPairEventQueue, refreshOrderbookSnapshotBatch

    # prev_event_u={}
    while True:
        try:
            if coinPair not in orderBookCoinPairEventQueue:
                continue

            remainingItems = int(serviceMgmt.serviceConfig['binanceconfig']['orderbook']['order_book_queue_chunk_size']) + 1
            while len(orderBookCoinPairEventQueue[coinPair]) > 0:
                remainingItems = remainingItems - 1
                if remainingItems <= 0:
                    break

                event = orderBookCoinPairEventQueue[coinPair].pop(0)
                # logging.info(f"Consumed Message ----> {message}")
                symbol = event['s']
                if symbol != coinSymbol:
                    continue

                if orderbook[symbol]['lastUpdateId'] == 0:
                    thread_lock.acquire()
                    try:
                        refreshOrderbookSnapshotBatch[symbol] = 1
                    finally:
                        thread_lock.release()
                    # set_orderbook_snapshot(symbol)

                lastUpdateId = int(orderbook[symbol]['lastUpdateId'])
                # prev_event_u[symbol] = event['u']
                if int(event['u']) <= lastUpdateId:
                    logging.info(f"Last updated id: {lastUpdateId}, Dropping event u: {event['u']}, s: {event['s']}")
                    continue

                if not (int(event['U']) <= lastUpdateId + 1 <= int(event['u'])):
                    logging.info(f"Dropping old and non sync events -> event_U {event['U']} event_u: {event['u']} Last Update id: {lastUpdateId}")
                    thread_lock.acquire()
                    try:
                        refreshOrderbookSnapshotBatch[symbol] = 1
                    finally:
                        thread_lock.release()
                    # set_orderbook_snapshot(symbol)
                    continue

                # if not (event['U'] == prev_event_u[symbol] + 1):
                #     logging.info(f"Events not in sequence. Dropping event {event['u']}")
                #     bufferConsumer.commit()
                #     continue

                orderbook[symbol]['lastUpdateId'] = int(event['u'])

                updateOrderBookData('bids',event['b'],symbol)
                updateOrderBookData('asks',event['a'],symbol)

                # logging.info(f"---------------Updated {symbol} orderbook------------")
                # logging.info(f" {symbol} orderbook ---->{orderbook}")
        except Exception as e:
            logging.exception('processEvents')
            logging.exception(json.dumps({'Status': 'Error', 'Msg': str(e)}))

        announceOrderbook(coinSymbol)

def orderbook_event_handler(wsapp, event):
    global orderBookCoinPairEventQueue, exchangecoinpairdata

    try:
        # logging.info("---------------------------------------------------")
        # logging.info(json.loads(event))
        event = json.loads(event)
        if('code' in event and 'msg' in event):
            logging.info(f"Orderbook Buffer Event Error: {event['msg']}")
            return

        if not (event['e'] and event['e'] == 'depthUpdate'):
            logging.info(f"Orderbook Buffer Event Error: Unknown Error")
            return

        if event['s'] is None or event['U'] is None or event['u'] is None or event['b'] is None or event['a'] is None:
            logging.info(f"Orderbook Buffer Event Error: Required Details are missing in event")
            return

        symbol = event['s']
        if symbol not in exchangecoinpairdata:
            return

        coinPair = exchangecoinpairdata[symbol]['pairname']
        orderBookCoinPairEventQueue[coinPair].append(event)
    except Exception as e:
        logging.exception("orderbook_event_handler")
        logging.exception(json.dumps({'Status': 'Error', 'Msg': str(e)}))

def init_socket(url, event_handler):
    try:
        websocket.enableTrace(True)
        wsapp = websocket.WebSocketApp(url, on_message=event_handler)
        wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    except Exception as e:
        wsapp.close()
        logging.exception("init_socket")
        logging.exception(json.dumps({'Status': 'Error', 'Msg': str(e)}))

def formatOrderBookData(type, symbol, reverse):
    orderList = []
    paircoin, basecoin = exchangecoinpairdata[symbol]['pairname'].split("/")
    ticksizep = exchangecoinpairdata[symbol]['ticksizep']
    ticksizeq = exchangecoinpairdata[symbol]['ticksizeq']
    orderedData = sorted(orderbook[symbol][type], reverse=reverse)
    for item in orderedData:
        order = orderbook[symbol][type][item]
        total = Decimal(order[0]) * Decimal(order[1])
        orderList.append(
            {"price": "{} {}".format(decimals_fill[ticksizep] % Decimal(order[0]), basecoin),
                "amount": "{} {}".format(decimals_fill[ticksizeq] % Decimal(order[1]), paircoin),
                "total": "{} {}".format(decimals_fill[ticksizep] % total, basecoin), "sum": "", "flash": "0"})
        if len(orderList) == int(serviceMgmt.serviceConfig['binanceconfig']['orderbook']['max_rows']):
            break
    return orderList

def announceOrderbook(symbol):
    global orderbookSnapshotStatus

    try:
        if symbol not in orderbookSnapshotStatus or not orderbookSnapshotStatus[symbol]:
            logging.info('Will not announce order book for symbol {}'.format(symbol))
        else:
            uisocketserviceurl = serviceMgmt.appInstanceUrlMap.get('uisocket_service')
            if uisocketserviceurl is None:
                logging.info('No active instance of the uisocket_service')
                raise Exception('No active instance of the uisocket_service')

            topbids = formatOrderBookData('bids', symbol, True)
            topasks = formatOrderBookData('asks', symbol, False)
            formatted_top_ask_bid = {"topbids": topbids, "topasks": topasks,"coinpair": exchangecoinpairdata[symbol]['pairname']}

            # logging.info(f"top_ask_bid ----> {formatted_top_ask_bid}")
            response=requests.post(
                url=f'{uisocketserviceurl}/tradeviewtopbidask', 
                data=json.dumps(formatted_top_ask_bid),
                headers={'Content-type': 'application/json', 'Accept': 'text/plain'},
                timeout=10,
                verify=False
            )
            # logging.info(f'UI socket publish response = {response.text}')
    except:
        logging.exception('announceOrderbook - {}'.format(symbol))

    time.sleep(int(serviceMgmt.serviceConfig['binanceconfig']['orderbook']['publish_orderbook_sleep_time_in_secs']))

def trade_event_handler(wsapp, event):
    global redisClient
    try:
        tradeData = json.loads(event)
        if tradeData['s'] not in exchangecoinpairdata:
            return

        transactionData = {
            "coinpair": exchangecoinpairdata[tradeData['s']]['pairname'],
            "id": str(tradeData['t']) + "-BN",
            "timeline": str(datetime.datetime.fromtimestamp(int(tradeData['T'])/1000).strftime('%Y-%m-%d %H:%M:%S')),
            "quantity": decimals_fill[exchangecoinpairdata[tradeData['s']]['ticksizeq']] % Decimal(tradeData['q']),
            "price": decimals_fill[exchangecoinpairdata[tradeData['s']]['ticksizep']] % Decimal(tradeData['p']),
            "total": decimals_fill[exchangecoinpairdata[tradeData['s']]['ticksizep']] % (Decimal(tradeData['p']) * Decimal(tradeData['q'])),
            "type": "Sell" if tradeData['m'] else "Buy"
        }

        # logging.info('Publishing the new trade data from Binance to redis pub-sub')
        redisClient.publish('trade_data_change', json.dumps(transactionData))
        time.sleep(int(serviceMgmt.serviceConfig['binanceconfig']['orderbook']['trade_data_handler_sleep_time_in_secs']))
    except Exception as e:
        logging.exception("trade_event_handler")
        logging.exception(json.dumps({'Status': 'Error', 'Msg': str(e)}))

def initKafkaProducer():
    global binanceProducer, orderbookProducer
    binanceProducer = Producer(
        kafkaServer=serviceMgmt.serviceConfig["kafka_config"]["server"],
        clientId=app_name,
        acks='all'
    )

    orderbookProducer = Producer(
        kafkaServer=serviceMgmt.serviceConfig["kafka_config"]["server"],
        clientId=app_name,
        acks='all'
    )
    
def monitorInternalThreads(thread_name,target_function,args=()):
    global binanceOrderAndEventThreadMap
    start = False
    if thread_name not in binanceOrderAndEventThreadMap:
        start = True
    else:
        threadAlive = binanceOrderAndEventThreadMap[thread_name].is_alive()
        if not threadAlive:
            start = True
    if start:
        logging.info(f'Initialising Thread : {thread_name}')
        t = threading.Thread(target=target_function,args=args,name=thread_name)
        t.daemon = True
        t.start()
        binanceOrderAndEventThreadMap[thread_name] = t

def monitorThreads():
    global binanceOrderAndEventThreadMap, orderPartitionCount, eventPartitionCount, exchangecoinpairdata

    apiClient = getAccount(account=serviceMgmt.serviceConfig["binanceconfig"]['account_to_trade'])

    monitorInternalThreads("createUserDataStreamSocket",createUserDataStreamSocket,(watchBinanceCyptoOrders,apiClient))
    monitorInternalThreads("processStaleOrders",processStaleOrders)
    monitorInternalThreads("validateOrder",validateOrder)
    monitorInternalThreads("checkAccountBalance",checkAccountBalance,(apiClient,))

    orderPartitionCount = serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_partition']
    for i in range(orderPartitionCount):
        monitorInternalThreads("processOrderConsumer"+str(i),processOrderConsumer,(i,apiClient)) 

    eventPartitionCount = serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_partition']
    for i in range(eventPartitionCount):
        monitorInternalThreads("processEventConsumer"+str(i),processEventConsumer,(i,))

    monitorInternalThreads("buffer_orderbook_events",init_socket,(orderbook_socket_url, orderbook_event_handler))
    monitorInternalThreads("buffer_trade_events",init_socket,(trade_socket_url, trade_event_handler))

    for symbol in exchangecoinpairdata:
        coinPair = exchangecoinpairdata[symbol]['pairname']
        monitorInternalThreads("process_orderbook_events_"+coinPair,process_orderbook_events,(symbol, coinPair,))

    monitorInternalThreads("refresh_orderbook_snapshot", refresh_orderbook_snapshot)

def initThreadFuncTargetMap():
    if serviceMgmt.serviceInfo['threads'] is None:
        return

    if not isinstance(serviceMgmt.serviceInfo['threads'], dict):
        raise Exception('Threads config is not a dict')

    funcNameList = []
    for threadName in serviceMgmt.serviceInfo['threads']:
        if 'function' not in serviceMgmt.serviceInfo['threads'][threadName]:
            raise Exception('Thread function not given')

        funcNameList.append(serviceMgmt.serviceInfo['threads'][threadName]['function'])

        if 'fargs' in serviceMgmt.serviceInfo['threads'][threadName]:
            funcNameList.append(serviceMgmt.serviceInfo['threads'][threadName]['fargs'])

        if 'fkwargs' in serviceMgmt.serviceInfo['threads'][threadName]:
            funcNameList.append(serviceMgmt.serviceInfo['threads'][threadName]['fkwargs'])

    threadFuncTargets = {}
    for f in funcNameList:
        if f not in globals():
            raise Exception('Thread function {} not defined'.format(f))

        funcObj = globals()[f]
        if not callable(funcObj):
            raise Exception('Thread function {} is not callable'.format(f))

        threadFuncTargets[f] = funcObj

    serviceMgmt.init({
        "threadFuncTargets": threadFuncTargets
    })


if __name__ == "__main__":
    try:
        threading.current_thread().name = "Main"
        appInstanceId = Gio.getUniqueId()
        init_logger(app_name=app_name, root_path=root_path, appInstanceId=appInstanceId)
        serviceMgmt.app_config = yaml.load(
            open(os.path.join(root_path, "conf", "setting", "app.yml"), "rt")
        )
        atexit.register(shutdownhook)

        giottus_service = (
            str(serviceMgmt.app_config["app_url"]["giottus_service"])
            + ":"
            + str(serviceMgmt.app_config["port"]["giottus_service"])
        )

        serviceMgmt.init({"name": app_name, "giottus_service": giottus_service})
        serviceMgmt.calibrateAppDataAndConfig()
        serviceMgmt.initAppInstanceInfo(appInstanceId=appInstanceId)
        settings = serviceMgmt.serviceInfo["settings"] 
        init_db(serviceMgmt.app_config[settings["db_str"]], settings["pool_size"])
        serviceMgmt.init({"ora_engine": get_engine()})
        
        initThreadFuncTargetMap()

        calibrateFuncList = OrderedDict([
            ('coins', calibrateCoins),
            ('notifications', calibrateNotification),
            ('tds', calibrateTdsConfigs),
            ('lowbalance_alert', calibrate_lowbalance_alert_config),
            ('messageQueues', calibrateMessageQueue)
        ])

        serviceMgmt.init({
            "calibrate_config": {
                "create_db_session": True,
                "pre_callback": calibrateAppDataAndConfigCallback,
                "func_list": calibrateFuncList,
            }
        })

        logging.info(serviceMgmt.calibrate())

        listeners = {
            "service_urls": None,
            "calibrate": None,
            "breakers": None,
            "routes": None,
            "threads": None
        }

        redisClient = redis.Redis(
            host=serviceMgmt.app_config["redisurl"], port=6379, db=0
        )
        initKafkaProducer()
        serviceMgmt.startThreads(redisClient=redisClient, listeners=listeners,cb=None)

        logging.info(
            "Starting the instance of {} with build number {}".format(
                app_name, serviceMgmt.appInstanceInfo["build"]
            )
        )
        orderPartitionCount = serviceMgmt.serviceConfig['binanceconfig']['kafka']['order_partition']
        eventPartitionCount = serviceMgmt.serviceConfig['binanceconfig']['kafka']['event_partition']
        serviceMgmt.app.run(
            host="0.0.0.0", port=serviceMgmt.serviceInfo["port"], debug=False, threaded=True
        )
    except Exception as e:
        logging.exception(
            f"Exception occurred while initialised {app_name} due to : {str(e)}"
        )
        traceback.print_exc()
        sys.exit(0)
