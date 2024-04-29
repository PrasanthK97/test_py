// import data from '../json/launchCoin.json' assert { type: 'json' };
import { embedJsonEditor, compactJson } from "./jsonHtmlEditor.js";
import {
  fetchCoinData,
  prepopulateForm,
  generateElements,
  randomHexGenerator,
  addListener,
  testAllRegex,
  populatePairCoin,
  coinLogoValidations,
  testFunction
} from "./launchCoin.util.js";

let logoImgTypes
let logoImgHeight
let logoImgWidth
let isValidFile = false
let base64String1
let imgdata = {}
let img 
let logoFlag = true
let uploadedFileType = []
let uploadedFileNameArray = []
let uploadedFileType2 = []
let storedFilesData

function loadForm() {
  // const form = document.getElementById("form");
  const form = document.getElementById("accordionExample")
  console.log(data);
  const jsonEditorIds = [];
  const relatedFields = {};

  function  readFormData(tableId, tableData) {
    // read data of the form based on the table data in json
    // const idArray = [];
    // tableData.forEach(el => idArray.push([el["uuid"], el["regex"]]));
    console.log(tableData)
    const row = {};
    tableData.forEach((el) => {
      const compid = el["uuid"];
      let id = tableId + "_" + compid;
      // let value = data[column];
      const column = compid;
      let value;
      // console.warn(id, value);
      const component = document.getElementById(id);
      if (component) {
        if(jsonEditorIds.includes(id)) {
          const editor = ace.edit(id);
          value = editor.getValue();
          try {
            value = compactJson(value);
            
          } catch (e) {
            console.error("invalid json");
          }
          row[column] = value;
        } else {
          const reArray = el["regex"];
          let [isValid, msg] = testAllRegex(reArray, component.value);
          msg = `Label: ${el["label"]}\nError: ${msg}`;
          if(isValid && el["required"] && !component.value && component.value=="") {
            isValid=false;
            msg = `Label: ${el["label"]}\nError: Mandatory field can't be empty`;
          }
          if(!isValid){
            throw msg;
          }
          row[column] = component.value;
          console.log(component, column,component.value)
        }
      } else {
        // console.error("component not found", id)
      }
    });
    let rows = []
    if(tableId=="SNxdU") {
        // a2QLA
        for(let i=0;i<7;i++) {
            let newRow = {...row, 'a2QLA': i};
            rows.push(newRow);
        }
    } else {
        rows=[row];
    }
    return rows;
  }

  async function createCoinLaunchEntry(tableName, rows, imgdata) {
    let s3FileNameArray
    console.log(imgdata)
    await $.ajax({
      context: this,
      url: '/coin-logo-upload',
      type: 'POST',
      contentType: 'application/json; charset=utf-8',
      traditional: true,
      success: function (response) {
        let data =  $.parseJSON(response)["fileType"]
        // uploadedFileName = $.parseJSON(response)["fileName"]
        s3FileNameArray= $.parseJSON(response)["fileNames"]
        console.log($.parseJSON(response)["fileNames"])
        // console.log(data)
        if(data.slice(0,3) != "png" && data.slice(0,3) != "svg"){
          logoFlag = false 
          alert("enter either a png or a svg file")
        }
        else{
          logoFlag = true

        }
      },
      error: function (error) {
          console.log(error, 'this is err');
          alert
      },
      data: JSON.stringify(imgdata),
    });    

    console.log(rows,rows[0], "rows type", typeof(rows))
    let s3Obj =  {}
    s3Obj.fileNames = s3FileNameArray
    // rows[0]["Th9YM"] = JSON.stringify(s3FileNameArray)
    var myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");
    var raw = JSON.stringify({
      tableName: tableName,
      tableData: rows
    });
    const requestOptions = {
      method: "POST",
      headers: myHeaders,
      body: raw,
    };

    if (logoFlag){
      let response = await fetch(`/launchCoin`, requestOptions);
      response = await response.json();
      if(response["Status"] == "Error") {
        alert(`Error:Failed to add rows to table\nMessage:${response["Msg"]}`)
    } else if(response["Status"] == "Success") {
        alert('Success:Added rows to table')
    }
    console.log(response);
    }
    else{
      alert("Invalid Logo Image File Type")
    }

  }

  function addRowtoTable(tableId, tableData) {
    console.log(tableData);
    // const jsonData = [...Object.keys(data[table_name]["mandatory"]), ...Object.keys(data[table_name]["default"])];
    try{
        const rows = readFormData(tableId, tableData);
        return rows
    } catch(err){
        console.log('error is....', err);
        alert(err);
    }
  }

  function publishFieldDependency() {
    // coin dropdown change listener
    let optionHtml = '<option value="">select</option>';
    console.log(coins, "_________coins")
    coins.forEach((el) => {
      optionHtml += `<option value="${el["code"]}">${el["name"]} (${el["code"]})</option>`;
    });
    const embedElement = document.getElementById("coins");
    embedElement.innerHTML = optionHtml;
    function coinChange(event) {
      // event.target.value
      const selectedCoinCode = event.target.value;
      console.log("coin changed", selectedCoinCode);
      if (selectedCoinCode) {
        fetchCoinData(selectedCoinCode, data).then((data) =>
          prepopulateForm(data, jsonEditorIds, coins)
        );
      }
    }
    addListener("coins", "change", coinChange);

    // erc20 slider listener
    console.log("erc20 changed");
    function erc20change(evt) {
      const erc20checked = evt.target.checked;
      console.log("erc20checked", erc20checked);
      // table_wallet_wallets => iaMFo
      // table_wallet_wallets_coin => wpkcf
      if (erc20checked) {
        document.getElementById("iaMFo").hidden = true;
        document.getElementById("wpkcf").hidden = false;
      } else {
        document.getElementById("iaMFo").hidden = false;
        document.getElementById("wpkcf").hidden = true;
      }
    }
    addListener("erc20", "click", erc20change);
  }

  function relatedFieldsProcess() {
    const updateChildField = (childId, parentId, relation, value) => {
      // relatedFieldsProcess
      const child = document.getElementById(childId);
      relation = relation.replace(parentId, value);
      child.value = eval(relation);
    };
    Object.keys(relatedFields).forEach((childId) => {
      const parentId = relatedFields[childId]["related_to"];
      const parent = document.getElementById(parentId);
      const relation = relatedFields[childId]["relation"];
      parent.addEventListener(
        "change",
        (event) =>
          updateChildField(childId, parentId, relation, event.target.value),
        false
      );
    });
  }
  data.forEach((el) => {
    
    // For each table iterate
    const tableElement = document.createElement("div");
    // tableName = el["tabuuid"]
    tableElement.setAttribute("id", el["tabuuid"]);
    tableElement.setAttribute("class", "row");
    if (el?.["isHidden"]) {
      // by default erc20 is on so wallets table will be hidden
      tableElement.hidden = true;
    }
    const mandatoryArray = el["data"].filter((element) => element.required);
    const defaultArray = el["data"].filter((element) => !element.required);
    tableElement.classList.add("row");
    console.log(el["label"])
    if(el["label"] === "coin_logo"){
      tableElement.innerHTML =`
      <div class="accordion-item">
            <h2 class="accordion-header" id="heading${el["label"]}">
                <button class="accordion-button collapsed accordian-heading"  type="button" data-bs-toggle="collapse"
                    data-bs-target="#collapse${el["label"]}" >
                    Table - ${el["label"]}
                </button>
            </h2>
        <div id="collapse${el["label"]}" class="accordion-collapse collapse " 
            data-bs-parent="#accordionExample">
            <div class= "coinLogoDropDown">
                <div>
                  <select id="coins-logo" class="coinDropdown"></select>
                </div>
                <div class="accordion-body" id="default_${el["tabuuid"]}">

                </div>
                
            </div>
        </div>
      </div>      
      `
      
      form.appendChild(tableElement);
      function logoCoinsSelectHandler(){
        let optionHtml = '<option value="">select</option>';
        console.log(coins, "_________coins")
        coins.forEach((el) => {
          optionHtml += `<option value="${el["code"]}">${el["name"]} (${el["code"]})</option>`;
        });
        const logoCoinsSelectEl = document.getElementById("coins-logo");
        logoCoinsSelectEl.innerHTML = optionHtml;

        logoCoinsSelectEl.addEventListener("change", function(event){
            console.log(event.target.value)
            let postData = {"coinName": event.target.value}
            $.ajax({
              context: this,
              url: '/coin-logo-data',
              type: 'POST',
              contentType: 'application/json; charset=utf-8',
              traditional: true,
              success: function (response) {  
                let data =  $.parseJSON(response)
                storedFilesData = data["data"] 
                console.log(data)
                console.log(Object.keys(storedFilesData).length)
                if(Object.keys(storedFilesData).length <2){
                  let uploadLogoBtn = document.createElement("button")
                  uploadLogoBtn.textContent = "Upload"
                  uploadLogoBtn.setAttribute("class", "btn btn-primary")
                  document.getElementById("logo-input-container").appendChild(uploadLogoBtn)
                  uploadLogoBtn.addEventListener("click", function(){
                    console.log(logoFlag,imgdata,uploadedFileNameArray, uploadedFileType2,  isValidFile, base64String1, img,  logoImgWidth, logoImgHeight)
                    testFunction()
                    coinLogoValidations(logoFlag,imgdata,uploadedFileNameArray, uploadedFileType2,  isValidFile, base64String1, img,  logoImgWidth, logoImgHeight);  
                 
                  })
                }
                // if(storedFilesData.length > 0){
                    for (let each in storedFilesData){
                      console.log(each)
                      // baseStr64="/9j/4AAQSkZJRgABAQE...";
                      let srcData
                      let imgId
                      if(each === "svg"){
                        srcData = "data:image/svg+xml;base64," + storedFilesData[each]
                        uploadedFileType2.push(each)
                        imgId = "data:image/svg+xml;base64,"
                      }
                      else if(each === "png"){
                        srcData = "data:image/png;base64," + storedFilesData[each]
                        uploadedFileType2.push(each)
                        imgId = "data:image/png;base64,"
                      }
                      
                      console.log(srcData)
                      let imgElem = document.createElement("img")
                      imgElem.setAttribute('src', srcData );
                      // imgElem.setAttribute("id", each )
                      document.getElementById("preview-image").appendChild(imgElem)
                      let deleteImageEl = document.createElement("button")
                      deleteImageEl.setAttribute("id", each)
                      deleteImageEl.textContent = "Delete Image"
                      deleteImageEl.setAttribute("class", "btn btn-primary")
                      document.getElementById("preview-image").appendChild(deleteImageEl)
                      deleteImageEl.setAttribute("style", "display: block; height: 40px;")
                      console.log("uploadedFileType2",uploadedFileType2)
                      deleteImageEl.addEventListener("click", function(){
                        console.log("dfdsnfjdsnflj")
                        logoFlag = true
                        let tt = uploadedFileType2.indexOf(deleteImageEl.getAttribute("id"))
                        uploadedFileType2.splice(uploadedFileType2.indexOf(deleteImageEl.getAttribute("id")), 1)
                        console.log(tt)
                        console.log("uploadedFileType2",uploadedFileType2)
                        document.getElementById("preview-image").removeChild(imgElem)
                        document.getElementById("preview-image").removeChild(deleteImageEl)
                        // document.getElementById('logo-file').value = ""
                       
                      }) 
                    }
                // }
              },
              error: function (error) {
                  console.log(error, 'this is err');
                  alert
              },
              data: JSON.stringify(postData),
            }); 
        })



      }

      // const mandatorySlot = document.getElementById(`mandatory_${el["tabuuid"]}`);
      // generateElements(
      //   mandatorySlot,
      //   mandatoryArray,
      //   el["tabuuid"],
      //   relatedFields,
      //   jsonEditorIds
      // );
      logoCoinsSelectHandler()
      const defaultSlot = document.getElementById(`default_${el["tabuuid"]}`);
  
      generateElements(
        defaultSlot,
        defaultArray,
        el["tabuuid"],
        relatedFields,
        jsonEditorIds
      );
    }
    else{
      tableElement.innerHTML = `
        <div class="accordion-item">
            <h2 class="accordion-header" id="heading${el["label"]}">
                <button class="accordion-button collapsed accordian-heading"  type="button" data-bs-toggle="collapse"
                    data-bs-target="#collapse${el["label"]}" >
                    Table - ${el["label"]}
                </button>
            </h2>
        <div id="collapse${el["label"]}" class="accordion-collapse collapse " 
            data-bs-parent="#accordionExample">
            <div class="accordion-body">
                <div class="bodyTopSection">
                ${
                  mandatoryArray.length > 0 &&
                  `<h4 >Mandatory</h4>
                    <div id="mandatory_${el["tabuuid"]}"></div>`
                }
                ${
                  defaultArray.length > 0 &&

                  `<div class = "default-button-container">
                        Default
                        <button class="expandCollapseBtn btn btn-info" type="button" data-toggle="collapse" data-target="#default_${el["tabuuid"]}" aria-expanded="false" aria-controls="collapseExample">
                            Expand/Collapse
                        </button>
                    </div>
                    <div class="collapse default-elements" id="default_${el["tabuuid"]}"></div>`  
                }
                <div class="key_value_horizontal add-row-to-table">
                    <button type="button" id="addRowtoTable_${
                      el["tabuuid"]
                    }" class="addRowBtn btn btn-success">
                        Add Row to Table
                    </button>
                </div>
                </div>
            </div>
            
        </div>
      </div>
        `;
    

    form.appendChild(tableElement);
    const tableAddRowBtnElement = document.getElementById(
      `addRowtoTable_${el["tabuuid"]}`
    );
    const tableId = el["tabuuid"];

    tableAddRowBtnElement.addEventListener(
      "click",
      function(){
        // const rows = addRowtoTable(tableId, el["data"]),false
        const rows = addRowtoTable(tableId, el["data"])
        console.log("244___________-------------------------")
        const fileInput = document.getElementById('logo-file');

        if((fileInput.value != "") && (logoFlag)){        
          imgdata = imgdata["base64String"].slice(0,2)
          console.log("img", imgdata)
          createCoinLaunchEntry(tableId, rows, imgdata);
        }
        else{
          // HERE COMES THE CODE WHICH HITS THE ADD TABLE BACKEND WHEN THE USER DOESN'T UPLOAD ANY COIN LOGO IMAGES.
          imgdata = []
          createCoinLaunchEntry(tableId, rows, imgdata);
        }
      }
    );

    // iterating through componenents and adding to default and mandatory
    const mandatorySlot = document.getElementById(`mandatory_${el["tabuuid"]}`);
    const defaultSlot = document.getElementById(`default_${el["tabuuid"]}`);
    generateElements(
      mandatorySlot,
      mandatoryArray,
      el["tabuuid"],
      relatedFields,
      jsonEditorIds
    );
    generateElements(
      defaultSlot,
      defaultArray,
      el["tabuuid"],
      relatedFields,
      jsonEditorIds
    );
    
    // load all the json editors for all jsonEditorIds
    embedJsonEditor(jsonEditorIds);
    document.addEventListener("click", function (e) {
      // check the id of the clicked element
      let id = e.target.id;
      if (id.includes("hex_")) {
        id = id.replace("hex_", "");
        // as the target is a link you want to prevent the default action of it
        e.preventDefault();
        document.getElementById(id).value = randomHexGenerator();
      }
    });
  }
   });

  //api call to get the image validation configurations.
  $.ajax({
    context: this,
    url: '/coin-logo-config',
    type: 'GET',
    contentType: "application/json; charset=utf-8",
    traditional: true,
    success: function (data) {
        try {
            data = $.parseJSON(data)["configData"];
            logoImgHeight = data["imgSize"]["height"]
            logoImgWidth = data["imgSize"]["width"]
            logoImgTypes = data["imgType"]
            console.log(logoImgHeight, logoImgWidth, logoImgTypes);
        } catch (e) {
            console.error("---------------------------------------");
        }
    },
    error: function () {
    }
  });
  relatedFieldsProcess();
  publishFieldDependency();
}

loadForm();
populatePairCoin();
// coinLogoValidations(logoFlag,imgdata,uploadedFileNameArray, uploadedFileType2,  isValidFile, base64String1, img,  logoImgWidth, logoImgHeight);  
