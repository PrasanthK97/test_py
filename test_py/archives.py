
@app.route("/coin-logo-data",methods=["POST"])
def coin_logo_data():
    response = {}
    data = request.get_json()
    logging.info("------------------------------el_---------{}".format(data["coinName"]))

    try:
        
        query = 'SELECT coin_logo_info FROM tradeengine.currency WHERE code = "{}";'.format(data["coinName"])
        logging.info("query .....")
        logging.info(query)
        result = g.revdbsession.execute(query)
        # g.revdbsession.flush()
        # g.revdbsession.commit()
        logging.info("------------------------------el_---------{}".format(result))
        for el in result:
            logging.info("------------------------------el_---------{}".format(el))
            # response["data"] = str(el)
            
        name_fix = "/coin-logo/"     
        logoPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + name_fix
        # downloaded = drive.downloadFile(bucket=app_config['s3kyc'], filename=filename, srcPath='selfieimg/' + str(int(id)) + '/' + str(file), key=s3KeyList['s3_key_kyc_download'])
        # downloaded = ["20240312T125239.917531.png", "20240312T125240.510585.svg"]
        downloaded = ["20240312T125240.510585.svg"]
        response["data"] = {}
        for eachFile in downloaded:
            # fileName = "/20240312T125239.917531.png"
            filePath = logoPath + str(eachFile)
            logoPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + name_fix
            # target_path = os.path.join(logoPath, filename)
            with open(filePath, "rb") as f:
                encoded_image = base64.b64encode(f.read())
                fileType = magic.Magic().from_buffer(base64.b64decode(encoded_image))
                fileType =  fileType.strip().split(" ")
                fileType = fileType[0].lower()
                logging.info("--------------------------------------------------------------------1353---------------logo----{}".format(fileType) ) 
                # response["data"][fileType] = str(encoded_image)
                response["data"][fileType] = {"fileName":str(eachFile), "b64Data": str(encoded_image)}


        # response["path"] = logoPath
        # response["file"] = encoded_image

        return json.dumps(response)
    except Exception as e:
        logging.info("------------------------------el_---------{}".format(e))
        return json.dumps({"Status": "Error", "Msg": "Unable to fetch the data"})











@app.route("/coin-logo-upload",methods=["POST"])
def coin_logo_upload(req_db_session = None):
    result = {}
    result["fileNames"] = []
    allowed_file_types = coinlogoconfig['file_mime_types_allowed']
    try:
        data = request.get_json()
        # data = json.loads(request.form.get('data'))
        logging.info("-------------------------1370-----------------{}".format(data))
        b64Data = data
        for encoded_data in b64Data:
            fileType = magic.Magic().from_buffer(base64.b64decode(encoded_data))
            logging.info("--------------------------------------------------------------------1353---------------logo----{}".format(encoded_data) ) 
            fileType =  fileType.strip().split(" ")
            fileType = fileType[0].lower()
            filename = str(datetime.datetime.now()).replace(' ', 'T').replace('-', '').replace(':', '') + '.' + fileType
            logging.info("--------------------------------------------------------------------1353---------------logo----{}".format(coinlogoconfig['file_mime_types_allowed']) ) 
        
            if(fileType in allowed_file_types):
                result["isTypeOk"] = True 
                result["fileType"] = fileType
                name_fix = "/coin-logo"
                logoPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + name_fix
                target_path = os.path.join(logoPath, filename)
                decodedData = base64.b64decode((encoded_data))

                if not os.path.exists(logoPath):
                    os.makedirs(logoPath)
                    imgFile = open(target_path, 'wb')
                    imgFile.write(decodedData)
                    
                else:
                    imgFile = open(target_path, 'wb')
                    imgFile.write(decodedData)

            else:
                result["Status"] = "Error"
                result["Msg"] = "Invalid File Type. Upload {} files".format(allowed_file_types)
                

            
            if drive.uploadLogo(srcpath=target_path, destfilename=filename, file_name_prefix = name_fix, bucket=app_config['coin_logo']['s3_bucket_name'], key=s3KeyList['s3_key_kyc_upload']):
            # if True:  
                    logging.info('Uploaded Successfully')
                    if req_db_session is None:
                        req_db_session = g.revdbsession
                    query_result = req_db_session.query(app_config_model.APPCONFIG).filter(app_config_model.APPCONFIG.name == 'coinlogoconfig').first()
                    config_result = json.loads(query_result.value)
                    if 'image_version' in config_result:
                        config_result['image_version'] = int(config_result['image_version']) + 1
                        logging.info("--------------version--------{}".format(config_result['image_version']))
                    else:
                        config_result['image_version'] = 1
                        logging.info("-----------version-----------{}".format(config_result['image_version']))
                    query_result.value = json.dumps(config_result)
                    req_db_session.commit()
                    giottus_calibrate_url = "{}/calibrate/all".format(giottusServiceUrl)
                    logging.info(giottus_calibrate_url)
                    service_response = requests.get(url=giottus_calibrate_url, timeout=5).json()
                    result["fileNames"].append(filename)

        return json.dumps(result)    
    except Exception as e:
        logging.info("-------------------------------------------------------------------------logo------{}".format(e))
        result["Status"] = "Error"

        return json.dumps(result)
