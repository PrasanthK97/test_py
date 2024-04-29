// // import data from '../json/launchCoin.json' assert { type: 'json' };
// import { embedJsonEditor, compactJson } from "./jsonHtmlEditor.js";
// import {
//   fetchCoinData,
//   prepopulateForm,
//   generateElements,
//   randomHexGenerator,
//   addListener,
//   testAllRegex,
//   populatePairCoin
// } from "./launchCoin.util.js";

// let logoImgTypes
// let logoImgHeight
// let logoImgWidth
// let base64img
// let isValidFile 

// function loadForm() {
//   // const form = document.getElementById("form");
//   const form = document.getElementById("accordion")
//   console.log(data);
//   const jsonEditorIds = [];
//   const relatedFields = {};

//   function readFormData(tableId, tableData) {
//     // read data of the form based on the table data in json
//     // const idArray = [];
//     // tableData.forEach(el => idArray.push([el["uuid"], el["regex"]]));
//     console.log("23")
//     const row = {};
//     tableData.forEach((el) => {
//       const compid = el["uuid"];
//       let id = tableId + "_" + compid;
//       // let value = data[column];
//       const column = compid;
//       let value;
//       // console.warn(id, value);
//       const component = document.getElementById(id);
//       if (component) {
//         if (jsonEditorIds.includes(id)) {
//           const editor = ace.edit(id);
//           value = editor.getValue();
//           try {
//             value = compactJson(value);
//           } catch (e) {
//             console.error("invalid json");
//           }
//           row[column] = value;
//         } else {
//           const reArray = el["regex"];
//           let [isValid, msg] = testAllRegex(reArray, component.value);
//           msg = `Label: ${el["label"]}\nError: ${msg}`;
//           if(isValid && el["required"] && !component.value && component.value=="") {
//             isValid=false;
//             msg = `Label: ${el["label"]}\nError: Mandatory field can't be empty`;
//           }
//           if(!isValid){
//             throw msg;
//           }
//           row[column] = component.value;
//         }
//       } else {
//         // console.error("component not found", id)
//       }
//     });
//     let rows = []
//     if(tableId=="SNxdU") {
//         // a2QLA
//         for(let i=0;i<7;i++) {
//             let newRow = {...row, 'a2QLA': i};
//             rows.push(newRow);
//         }
//     } else {
//         rows=[row];
//     }
//     return rows;
//   }

//   async function createCoinLaunchEntry(tableName, rows) {
//     var myHeaders = new Headers();
//     myHeaders.append("Content-Type", "application/json");
//     var raw = JSON.stringify({
//       tableName: tableName,
//       tableData: rows,
//     });

//     const requestOptions = {
//       method: "POST",
//       headers: myHeaders,
//       body: raw,
//     };
//     let response = await fetch(`/launchCoin`, requestOptions);
//     response = await response.json();
//     if(response["Status"] == "Error") {
//         alert(`Error:Failed to add rows to table\nMessage:${response["Msg"]}`)
//     } else if(response["Status"] == "Success") {
//         alert('Success:Added rows to table')
//     }
//     console.log(response);
//   }

//   function addRowtoTable(tableId, tableData) {
//     console.log(tableData);
//     // const jsonData = [...Object.keys(data[table_name]["mandatory"]), ...Object.keys(data[table_name]["default"])];
//     try{
//         const rows = readFormData(tableId, tableData);
//         createCoinLaunchEntry(tableId, rows);
//     } catch(err){
//         console.log('error is....', err);
//         alert(err);
//     }
//   }

//   function publishFieldDependency() {
//     // coin dropdown change listener
//     let optionHtml = '<option value="">select</option>';
//     console.log(coins, "_________coins")
//     coins.forEach((el) => {
//       optionHtml += `<option value="${el["code"]}">${el["name"]} (${el["code"]})</option>`;
//     });
//     const embedElement = document.getElementById("coins");
//     embedElement.innerHTML = optionHtml;
//     function coinChange(event) {
//       // event.target.value
//       const selectedCoinCode = event.target.value;
//       console.log("coin changed", selectedCoinCode);
//       if (selectedCoinCode) {
//         fetchCoinData(selectedCoinCode, data).then((data) =>
          
//           prepopulateForm(data, jsonEditorIds, coins)
//         );
//       }
//     }
//     addListener("coins", "change", coinChange);

//     // erc20 slider listener
//     console.log("erc20 changed");
//     function erc20change(evt) {
//       const erc20checked = evt.target.checked;
//       console.log("erc20checked", erc20checked);
//       // table_wallet_wallets => iaMFo
//       // table_wallet_wallets_coin => wpkcf
//       if (erc20checked) {
//         document.getElementById("iaMFo").hidden = true;
//         document.getElementById("wpkcf").hidden = false;
//       } else {
//         document.getElementById("iaMFo").hidden = false;
//         document.getElementById("wpkcf").hidden = true;
//       }
//     }
//     addListener("erc20", "click", erc20change);
//   }

//   function relatedFieldsProcess() {
//     const updateChildField = (childId, parentId, relation, value) => {
//       // relatedFieldsProcess
//       const child = document.getElementById(childId);
//       relation = relation.replace(parentId, value);
//       child.value = eval(relation);
//     };
//     Object.keys(relatedFields).forEach((childId) => {
//       const parentId = relatedFields[childId]["related_to"];
//       const parent = document.getElementById(parentId);
//       const relation = relatedFields[childId]["relation"];
//       parent.addEventListener(
//         "change",
//         (event) =>
//           updateChildField(childId, parentId, relation, event.target.value),
//         false
//       );
//     });
//   }
//   data.forEach((el) => {
    
//     // For each table iterate
//     const tableElement = document.createElement("div");
//     // tableName = el["tabuuid"]
//     tableElement.setAttribute("id", el["tabuuid"]);
//     tableElement.setAttribute("class", "row");
//     if (el?.["isHidden"]) {
//       // by default erc20 is on so wallets table will be hidden
//       tableElement.hidden = true;
//     }
//     const mandatoryArray = el["data"].filter((element) => element.required);
//     const defaultArray = el["data"].filter((element) => !element.required);
//     tableElement.classList.add("row");
//     // tableElement.innerHTML = `
//     //     <h2 class="tableheader">Table - ${el["label"]}</h2>
//     //     ${
//     //       mandatoryArray.length > 0 &&
//     //       `<h4>Mandatory</h4>
//     //         <div id="mandatory_${el["tabuuid"]}"></div>`
//     //     }
//     //     ${
//     //       defaultArray.length > 0 &&
//     //       `<h4>
//     //             Default
//     //             <button class="btn btn-info" type="button" data-toggle="collapse" data-target="#default_${el["tabuuid"]}" aria-expanded="false" aria-controls="collapseExample">
//     //                 Expand/Collapse
//     //             </button>
//     //         </h4>
//     //         <div class="collapse" id="default_${el["tabuuid"]}"></div>`
//     //     }
//     //     <div class="key_value_horizontal add-row-to-table">
//     //         <button type="button" id="addRowtoTable_${
//     //           el["tabuuid"]
//     //         }" class="btn btn-success">
//     //             Add Row to Table
//     //         </button>
//     //     </div>
//     //     `;

//      tableElement.innerHTML = `
//         <div class="accordion-item">
            
//                 <button class="accordion-button collapsed accordian-heading"  type="button" data-bs-toggle="collapse"
//                     data-bs-target="#collapse${el["label"]}" >
//                     Table - ${el["label"]}
//                 </button>
            
//         <div id="collapse${el["label"]}" class="accordion-collapse collapse " 
//             data-bs-parent="#accordionExample">
//             <div class="accordion-body">
//                 <div class="bodyTopSection">
//                 ${
//                   mandatoryArray.length > 0 &&
//                   `<h4>Mandatory</h4>
//                     <div id="mandatory_${el["tabuuid"]}"></div>`
//                 }
//                 ${
//                   defaultArray.length > 0 &&
//                   `<h4>
//                         Default
//                         <button class="expandCollapseBtn btn btn-info" type="button" data-toggle="collapse" data-target="#default_${el["tabuuid"]}" aria-expanded="false" aria-controls="collapseExample">
//                             Expand/Collapse
//                         </button>
//                     </h4>
//                     <div class="collapse" id="default_${el["tabuuid"]}"></div>`
//                 }
//                 <div class="key_value_horizontal add-row-to-table">
//                     <button type="button" id="addRowtoTable_${
//                       el["tabuuid"]
//                     }" class="addRowBtn btn btn-success">
//                         Add Row to Table
//                     </button>
//                 </div>
//                 </div>
//             </div>
            
//         </div>
//       </div>
//         `;



//     form.appendChild(tableElement);
//     const tableAddRowBtnElement = document.getElementById(
//       `addRowtoTable_${el["tabuuid"]}`
//     );
//     const tableId = el["tabuuid"];
//     tableAddRowBtnElement.addEventListener(
//       "click",
//       () => addRowtoTable(tableId, el["data"]),
//       false
//     );
//     // iterating through componenents and adding to default and mandatory
//     const mandatorySlot = document.getElementById(`mandatory_${el["tabuuid"]}`);
//     const defaultSlot = document.getElementById(`default_${el["tabuuid"]}`);
//     generateElements(
//       mandatorySlot,
//       mandatoryArray,
//       el["tabuuid"],
//       relatedFields,
//       jsonEditorIds
//     );
//     generateElements(
//       defaultSlot,
//       defaultArray,
//       el["tabuuid"],
//       relatedFields,
//       jsonEditorIds
//     );

//     // load all the json editors for all jsonEditorIds
//     embedJsonEditor(jsonEditorIds);
//     document.addEventListener("click", function (e) {
//       // check the id of the clicked element
//       let id = e.target.id;
//       if (id.includes("hex_")) {
//         id = id.replace("hex_", "");
//         // as the target is a link you want to prevent the default action of it
//         e.preventDefault();
//         document.getElementById(id).value = randomHexGenerator();
//       }
//     });
//   });


// //api call to get the image validation configurations.
//   $.ajax({
//     context: this,
//     url: '/coin-logo-config',
//     type: 'GET',
//     contentType: "application/json; charset=utf-8",
//     traditional: true,
//     success: function (data) {
//         try {
//             data = $.parseJSON(data)["configData"];
//             logoImgHeight = data["imgSize"]["height"]
//             logoImgWidth = data["imgSize"]["width"]
//             logoImgTypes = data["imgType"]
//             console.log(logoImgHeight, logoImgWidth, logoImgTypes);
//         } catch (e) {
//             console.error("---------------------------------------");
//         }
//     },
//     error: function () {
//     }
// });

//   relatedFieldsProcess();
//   publishFieldDependency();
// }



// let logoFormEl = document.getElementById("logoForm")
// // logoFormEl.addEventListener('change', (event) => {
// //   const fileList = event.target.files;
// //   var fileReader = new FileReader();

// //   console.log(fileList);



// //   fileReader.onload = function () {
// //     base64String = fileReader.result.replace("data:", "")
// //         .replace(/^.+,/, "");

// //     imageBase64Stringsep = base64String;
// //     // alert(imageBase64Stringsep);
// //     console.log(base64String);
// // }
// // fileReader.readAsDataURL(fileList);

// // });


// let flag = true

// function ajaxCall(imgdata){
//   $.ajax({
//     context: this,
//     url: '/coin-logo-upload',
//     type: 'POST',
//     contentType: 'application/json; charset=utf-8',
//     traditional: true,
//     success: function (response) {
//       let data =  $.parseJSON(response)["fileType"]
//       let typeValidation = $.parseJSON(response)["isTypeOk"]
//       console.log(typeValidation)
//       console.log(data)
//       if(data.slice(0,3) != "png" && data.slice(0,3) != "svg"){
//         alert("enter either a png or a svg file")
//       }
//     },
//     error: function (error) {
//         console.log(error, 'this is err');
//     },
//     data: JSON.stringify(imgdata),
//   });  
// }


  







// // function sendingImageFile(selectedFile){
// //   const formData = new FormData();
// //   formData.append("file", JSON.stringify(selectedFile));
// //   return formData
// // }

// // const fileToPass = sendingImageFile()

// // console.log(formData)
// // function ajaxCall(selectedFile){
// //   fetch('/coin-logo-upload', {
// //     method: 'post',
// //     headers: {
// //         'enctype' : 'application/json; charset=utf-8',
// //         'traditional': true,
// //         'cache': false,
// //         'processData': false,
// //     },
// //     body: JSON.stringify({"data": "nothing"})
// // }).then(response => {
// //     if(response.ok) {
// //         return response.json();
// //     }
// //     throw new Error(errorMessageFetch);
// // }).then(result => {
    
// //     alert(result.Msg);
// //     if (result.Status == 'Success') {
        
// //     }
// // }).catch(err => {
// //     alert(err);
// // }).finally(() => {
// //     console.log('saveContest call completed');
// // });
// // }




// function backEndFileTypeValidation(fileReader, selectedFile){
//   // var fileReader = new FileReader();


//   fileReader.onload =   function () {
//     var base64String =   fileReader.result.replace("data:", "")
//         .replace(/^.+,/, "");
//     console.log("=================",base64String)
//     let imageBase64Stringsep =  base64String;
//     base64String1 =  base64String
//     // alert(imageBase64Stringsep);

//     console.log(base64String1);
//     imgdata.base64String = base64String1
//     console.log(imgdata)
//     ajaxCall(imgdata)  
//     // var fullB64Data = `data:image/png;base64,${base64String1}`
//     // Base64ToImage(fullB64Data)
//   }
//   fileReader.readAsDataURL(selectedFile);
// }


// function frontEndFileTypeValidation(selectedFile, img){
//       // var blob = files[i];
//        // See step 1 above
//        console.log("water")
       
//       var fileReader = new FileReader();
    
//       fileReader.onloadend = function(e) {
//         if(flag){
//           console.log(flag)
//           flag = false
//           let isValidFile = false;
//           console.log("water1.5")
//           var arr = (new Uint8Array(e.target.result)).subarray(0, 4);
//           console.log(arr)
//           console.log(e.target.result)
//           // var arr = (new Uint8Array(selectedFile.target.result)).subarray(0, 4);
//           var header = "";
//           for(var i = 0; i < arr.length; i++) {
//             header += arr[i].toString(16);
//           }
//           console.log(header);
//           // Check the file signature against known types
//           switch (header) {
//             case "89504e47":
//                 // type = "image/png";
//                 isValidFile = true
//                 console.log("png")
//                 break;

//             case "3c3f786d":
//             case "3c737667":
//                 isValidFile = true
//                 console.log("svg")
//                 // type = "image/svg";
//                 break;
//             default:
//                 // type = "unknown"; // Or you can use the blob.type as fallback
//                 break;
//           }
//           console.log("water2")
//           if(isValidFile){
//             console.log("Valid file type", this)
//             backEndFileTypeValidation(fileReader, selectedFile)
//             document.getElementById('main').appendChild(img); 

//           }
//           else{
//             console.log("Invalid file type", this)
//             alert("Invalid file type")
//           }
//         }
      
//       };
//       console.log("water3")
//       fileReader.readAsArrayBuffer(selectedFile);
      

//       // if(isValidFile){
//       //   console.log("Valid file type")
//       //   backEndFileTypeValidation()
//       // }
//       // else{
//       //   console.log("Invalid file type")
//       // }
// }




// logoFormEl.addEventListener("submit", function(event){
//   event.preventDefault()
//   console.log("clicked")
//   const fileInput = document.getElementById('logo-input-el');
//   // console.log(fileInput)
//   if(fileInput.value != ""){

//     isValidFile = false
//     flag = true
//     var selectedFile = fileInput.files[0];
//     console.log(selectedFile.type)
//     console.log(selectedFile)
//     var fileReader1 = new FileReader();
    
//     fileReader1.onload =   function () {
//       var base64String =   fileReader1.result.replace("data:", "")
//           .replace(/^.+,/, "");
//       console.log("+++++++++++++++++++++++++++",base64String)
//       base64String1 =  base64String
//       console.log(base64String1);
//       imgdata.base64String = base64String1  
//       var fullB64Data = `data:image/png;base64,${base64String1}`
//       // Base64ToImage(fullB64Data)
//       var img = new Image();
//       img.src = fullB64Data;
//       img.onload = function() {
//       console.log(this.width, this.height,  "---------------------------")
//       // if(this.width <= logoImgWidth && this.height <= logoImgHeight)
//       if(true){
        
//         frontEndFileTypeValidation(selectedFile, img) 
//         console.log(isValidFile)  
//         // if(isValidFile){
//         //   console.log(isValidFile)
//         //   document.getElementById('main').appendChild(img); 
//         // } 

//       }
//       else{
//         alert(`Required Dimensions are Height x Width: ${logoImgHeight}px x ${logoImgWidth}px`);
//       }
//     }
//     fileInput.value = null
//     }
//     // fileReader.readAsDataURL(selectedFile);
//     // var blob = fileInput.files[0]; // See step 1 above
//     // var fileReader = new FileReader();
//     // backEndFileTypeValidation(fileReader, selectedFile)
//     // if(isValidFile){
//     //   backEndFileTypeValidation(fileReader, selectedFile)
//     // }
//     // else{
//     //   console.log("==================")
//     // }
//     // fileReader.readAsDataURL(blob);
//     fileReader1.readAsDataURL(selectedFile);
//     // console.log(base64String1)
//   }
//   else{
//     alert("Select an image file to upload")
//   }
// })



// // function Base64ToImage(base64img) {
// //   var img = new Image();
// //   img.src = base64img;
// //   img.onload = function() {
// //     console.log(this.width, this.height, "---------------------------")
// //     if(this.width <= logoImgWidth && this.height <= logoImgHeight){
// //       document.getElementById('main').appendChild(img); 
// //     }
// //     else{
// //       alert(`Required Dimensions are Height x Width: ${logoImgHeight}px x ${logoImgWidth}px`);
// //     }
// //   }
// //   }

// var base64String1 = "ttttt"
// const imgdata = {}

// // const fileInput = document.getElementById('logo-input-el');
// // fileInput.onchange = () => {
// //     // var selectedFile = fileInput.files[0];
// //     console.log(selectedFile.type)
// //     console.log(selectedFile)
// //     frontEndFileTypeValidation(selectedFile)    

// // // var blob = fileInput.files[0]; // See step 1 above
// // var fileReader = new FileReader();

// // if(isValidFile){
// //   fileReader.onload =   function () {
// //           var base64String =   fileReader.result.replace("data:", "")
// //               .replace(/^.+,/, "");
// //           console.log(base64String)
// //           let imageBase64Stringsep =  base64String;
// //           base64String1 =  base64String
// //           // alert(imageBase64Stringsep);

// //           console.log(base64String1);
// //           imgdata.base64String = base64String1
// //           console.log(imgdata)
// //           ajaxCall(imgdata)

// //           var fullB64Data = `data:image/png;base64,${base64String1}`
// //           Base64ToImage(fullB64Data)
// //   }
// // }
// // // fileReader.readAsDataURL(blob);
// // fileReader.readAsDataURL(selectedFile);

// // console.log(base64String1)
// // }

 



// // function getBase64Img() {
// //     return 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFAAAABQBAMAAAB8P++eAAAAMFBMVEX////7+/vr6+vNzc2qqqplywBWrQD/Y5T/JTf+AADmAABeeEQvNydqAAAGBAMAAAGoF14oAAAEBUlEQVR42u2WTUwUVxzA/2925LIojsyqCLtMqSQeFJeFROPHstjdNkZdkaiX9mRTovYgSYNFE43Rg5IeNG5SqyaVcGzXkvTm7hZWq9bKriAYYzSLBI3WAplp0jY2kV3fzPuYGcBxD/bmO0zevPeb//f7v0FQ5EDvwf8LFG89ixYDol9Wo+smKal2sHF6kM6q7qbE5tY03d7SHR6ygiuvwY1txkzIlPaD3NQ4Zryk/PCP1wKibBlI97dq+idXUxqgcEkVnruS/smRpuWaCbrHU5pc/wyTQsata0U77wVBTlRP9UNkT9oE61K9AHL9VERbeTVurMjNm54mqrFw8CwKmWBfpf6V2Dx/TdxN3YjMf2JwIEZ8JnjnEbEjvLAQZ1EKlxkciJ9UcdD1Z5ztDmgsgPIrMkWtMgfd41xOYXZOIjUcLB3tdchypHWIgb5k2gH0nE0zMHjRCRTLWxgY/VQrDuybcAQ/DDEwmwEn8COlSDBcTUE0GXcEjRzqoDDhDH7sfccg2szAt9hogjBFQFRmDyd9N1VTEFegXXKEVKQFJHEUd0DSKhLthIm0HSQpnNdCNizgq147GP0m7QTygNMye6NqM9ekwlG4kLI5I4cMZ+bVhMA8MwX1rw9oIjakAYX6jXlBRRJ4BF6PwmRSfQyLFhJwWy/IG8khKuirljODG0DfGAd3qCBNEol5vCqFAxqYLeUyB0H2A2Q0DspRD3BQfJEc5KBl6KprG6pNEPqqfkKzOQBsha2bQeVw6g3nSySaGYgb99ylhsLDLVYQ3E+oozOGHKzVbCBs+HkuUm6ivd4E0ZYeNTPTTrn51+0wA9TJgpZVLeaV1y+4yS8c64W0/AcFTBCHK/fFEMwFAmo4KRldE0ZxsF8cGbTuzYqHtOz8t6e8TMk5CcjNSC+kdcfgPyag9AoDhUQARmvAcIiAW3v0JzWJg64BJXdBQ21+/b4zwMqRW5dA2h3I7+onYJd+Y4i3FbyMxz4hxE7hS2MBdSq6Fgq6Bny/dxv6Sk76COj6Yz91ZG8dToQ7oYNCqu4QjX9FK5UoPt/PXO701mruhwcxGP2ecahzzxBtUtlzLHclsXtBA6wc+a2brlUc8TCvg5cPM7LiaOPT8Y4ub3bpAbqy+MQ18xRmvIe5nr83YXDt8Nca0/GveWniSHByybEVwx1dD1ccYB/6yF8DTaGcYLFA391o6DjjohaWfKXQ/xCWazmhPDhNbYf2WH4fse9zhRUkLwrXlUD+9iVD5HR77KWheclxkqsZ1bM+pqcWZ2xVe0zXjPb6pyO8IK1l5trcA7mLKuo48+VpQG0BuPmZeTbs9Vi+9oQyqv6Ieyou4FybU+HqRS4o+TH1VG7MvgFFjvfguwFfA4FobmCxcnTPAAAAAElFTkSuQmCC';
   
// // }
// // base64img = getBase64Img();




// // Base64ToImage(base64img)



// loadForm();
// populatePairCoin();




// // case "json":
// //       jsonEditorIds.push(id);
// //       return `<div class="json">${getJsonEditor(id, elementData["default"])}</div>`;
// //     case "randomhex":
// //       return `<input ${getAttributes(
// //         id
// //       )} type="color" value="${randomHexGenerator()}" style="height: 50px;width: 50px;">
// //                     <button 
// //                         id="hex_${id}"
// //                         type="button"
// //                         class = "btn btn-danger"
// //                         >
// //                         Try Different Color
// //                     </button>`;
// //     case "dropdown":
// //       return `<select ${getAttributes(id)} name="dropdown">
// //                         ${constructOptions(elementData)}
// //                     </select>`;



// import data from '../json/launchCoin.json' assert { type: 'json' };
import { embedJsonEditor, compactJson } from "./jsonHtmlEditor.js";
import {
  fetchCoinData,
  prepopulateForm,
  generateElements,
  randomHexGenerator,
  addListener,
  testAllRegex,
  populatePairCoin
} from "./launchCoin.util.js";

let logoImgTypes
let logoImgHeight
let logoImgWidth
let isValidFile = false
let base64String1
// let base64String
let imgdata = {}
// let selectedFile
let img 
let i = true
let logoFlag = true
// let fileCount = 0
let uploadedFileType = []
let uploadedFileNameArray = []

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
    console.log("23")
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
        if (jsonEditorIds.includes(id)) {
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
          //****************************************************remove await and append the other POST request here *********** */
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
    rows[0]["Th9YM"] = JSON.stringify(s3FileNameArray)
    var myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");
    var raw = JSON.stringify({
      tableName: tableName,
      tableData: rows
      // logoData :imgdata
    });
    console.log("[[[[[[[[[[[[[[[[[[[[[[[", imgdata, rows)
    const requestOptions = {
      method: "POST",
      headers: myHeaders,
      body: raw,
    };
    //******************************************only one success condition*******consider logoFlag************ */
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

    // let response = await fetch(`/launchCoin`, requestOptions);
    // response = await response.json();
    // if(response["Status"] == "Error") {
    //     alert(`Error:Failed to add rows to table\nMessage:${response["Msg"]}`)
    // } else if(response["Status"] == "Success") {
    //     alert('Success:Added rows to table')
    // }
    // console.log(response);
  }

  function addRowtoTable(tableId, tableData) {
    console.log(tableData);
    // const jsonData = [...Object.keys(data[table_name]["mandatory"]), ...Object.keys(data[table_name]["default"])];
    try{
        const rows = readFormData(tableId, tableData);
        return rows
        // createCoinLaunchEntry(tableId, rows, imgdata);
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
    // i = i +1 
      // console.log(i)
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
        // createCoinLaunchEntry(tableId, rows, imgdata);
        console.log("244___________-------------------------")
    const fileInput = document.getElementById('logo-file');

      if(fileInput.value != ""){
        // isValidFile = false
        // flag = true
        // var selectedFile = fileInput.files[0];
        // selectedFile = fileInput.files[0];

        // console.log(selectedFile.type)
        // console.log(selectedFile)
        // var fileReader1 = new FileReader();

        console.log(isValidFile, flag, img)

        if((logoFlag)){
          console.log(isValidFile, flag, img)
          
          imgdata = imgdata["base64String"].slice(0,2)
          console.log("img", imgdata)
          createCoinLaunchEntry(tableId, rows, imgdata);
        }

      }
      else{
        // HERE COMES THE CODE WHICH HITS THE ADD TABLE BACKEND WHEN THE USER DOESN'T UPLOAD ANY COIN LOGO IMAGES.
        imgdata = []

        createCoinLaunchEntry(tableId, rows, imgdata);
        // alert("Select an image file to upload")
      }
       
      
      // () => addRowtoTable(tableId, el["data"]),
      // false
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
    
    if(i){
      i = false
      // i = true
    // document.getElementById("preview-button").addEventListener("click", function(event){

      // event.preventDefault()
      console.log("clicked")
      const fileInput = document.getElementById('logo-file');
      // console.log(fileInput)
      // i = i +1 
      // console.log(i)
    // if(fileCount <=2){
      fileInput.onchange = function(){
        logoFlag = true 
        console.log("Onchange clicked", uploadedFileType.length)
       
      
        if(fileInput.value != ""){
          isValidFile = false
          flag = true
          var selectedFile = fileInput.files[0];
          let fileExtn = selectedFile.type
          console.log(selectedFile.type)
          console.log(selectedFile)
          var fileReader1 = new FileReader();
        
        if(uploadedFileType.length <2){
          fileReader1.onload =   function () {
            var base64String =   fileReader1.result.replace("data:", "")
                .replace(/^.+,/, "");
            console.log("+++++++++++++++++++++++++++",base64String)
            base64String1 =  base64String
            console.log(base64String1);
            // imgdata.base64String = base64String1  
            // var fullB64Data = `data:image/png;base64,${base64String1}`

            var fullB64Data = `data:${fileExtn};base64,${base64String1}`


            // Base64ToImage(fullB64Data)
            // var img = new Image();
            img = new Image();
            img.src = fullB64Data;
            img.onload = function() {
            console.log(this.width, this.height,  "---------------------------")
            // if(this.width <= logoImgWidth && this.height <= logoImgHeight)
            // triggerFrontValidation = (selectedFile, img) => {
            //   console.log("319----------------")
            //   frontEndFileTypeValidation(selectedFile, img) 
            // }
            
            if(true){
              
              let toHitBackEnd = true
              // previewValidation(selectedFile, img)
              frontEndFileTypeValidation(selectedFile, img, toHitBackEnd)
              console.log(isValidFile, "img", img)  
            }
            else{
              alert(`Required Dimensions are Height x Width: ${logoImgHeight}px x ${logoImgWidth}px`);
            }
            }
           // fileInput.value = null
          }
        }
          fileReader1.readAsDataURL(selectedFile);
        }
        else{
        // alert("Select an image file to upload")
        
        }
      
        // });
      }
    // }
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


async function ajaxCall(   imgdata){
 await $.ajax({
    context: this,
    url: '/coin-logo-upload',
    type: 'POST',
    contentType: 'application/json; charset=utf-8',
    traditional: true,
    success: function (response) {
      let data =  $.parseJSON(response)["fileType"]
      let typeValidation = $.parseJSON(response)["isTypeOk"]
      // console.log(typeValidation)
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
    },
    data: JSON.stringify(imgdata),
  });  
  console.log(isValidFile, logoFlag)
}

function backEndFileTypeValidation(fileReader, selectedFile ){
  // var fileReader = new FileReader();
  fileReader.onload =   function () {
    var base64String =   fileReader.result.replace("data:", "")
        .replace(/^.+,/, "");
    console.log("=================",base64String)
    let imageBase64Stringsep =  base64String;
    base64String1 =  base64String
    // alert(imageBase64Stringsep);
    console.log(base64String1);
    // imgdata.base64String = []
    uploadedFileNameArray.push(base64String1)
    imgdata.base64String = uploadedFileNameArray
    // console.log(imgdata)
    // ajaxCall( imgdata) 
    // var fullB64Data = `data:image/png;base64,${base64String1}`
    // Base64ToImage(fullB64Data)
  }
  fileReader.readAsDataURL(selectedFile);
}

// function previewValidation(selectedFile, img){
//   console.log("water")
       
//   var fileReader = new FileReader();

//   fileReader.onloadend = function(e) {
//     if(flag){
//       console.log(flag)
//       flag = false
//       let isValidFile = false;
//       console.log("water1.5")
//       var arr = (new Uint8Array(e.target.result)).subarray(0, 4);
//       // console.log(arr)
//       // console.log(e.target.result)
//       var header = "";
//       for(var i = 0; i < arr.length; i++) {
//         header += arr[i].toString(16);
//       }
//       console.log(header);
//       // Check the file signature against known types
//       switch (header) {
//         case "89504e47":
//             isValidFile = true
//             console.log("png")
//             break;

//         case "3c3f786d":
//         case "3c737667":
//             isValidFile = true
//             console.log("svg")
//             break;
//         default:
//             break;
//       }
//       console.log("water2")
//       if(isValidFile){
//         // console.log("Valid file type", this)
//         // document.getElementById('main').appendChild(img); 
//         document.getElementById("preview-image").appendChild(img)

//       }
//       else{
//         console.log("Invalid file type", this)
//         alert("Invalid file type")
//       }
//     }
  
//   };
//   console.log("water3")
//   fileReader.readAsArrayBuffer(selectedFile);
// }

let flag = true
function frontEndFileTypeValidation(selectedFile, img, toHitBackEnd){
      // var blob = files[i];
       // See step 1 above
       console.log("water")
       
      var fileReader = new FileReader();
    
      fileReader.onloadend = function(e) {
        if(flag){
          console.log(flag)
          flag = false
          let isValidFile = false;
          console.log("water1.5")
          var arr = (new Uint8Array(e.target.result)).subarray(0, 4);
          // console.log(arr)
          // console.log(e.target.result)
          var header = "";
          for(var i = 0; i < arr.length; i++) {
            header += arr[i].toString(16);
          }
          console.log(header);
          // Check the file signature against known types
          let currentFileType
          switch (header) {
            case "89504e47":
                isValidFile = true
                console.log("png")
                currentFileType = "png"
                
                break;

            case "3c3f786d":
            case "3c737667":
                isValidFile = true
                console.log("svg")
                currentFileType = "svg"
              
                break;
            default:
                break;
          }
          console.log("water2", isValidFile, uploadedFileType)
          if(isValidFile){
            // console.log("Valid file type", this)
            // document.getElementById('main').appendChild(img); 
              if((uploadedFileType.indexOf("png") !== -1) && (currentFileType === "png") && (uploadedFileType.length === 1)){
                    alert("Upload a svg file")
              }
              else if((uploadedFileType.indexOf("svg") !== -1) && (currentFileType === "svg") && (uploadedFileType.length === 1)){
                    alert("Upload a png file")
              }
              else{
                uploadedFileType.push(currentFileType)
                console.log(uploadedFileType)
                document.getElementById("preview-image").appendChild(img)
                if(toHitBackEnd){
                  console.log("hitBE")
                  backEndFileTypeValidation(fileReader, selectedFile)
                }
    
              }
            
          }
          else{
            logoFlag = false
            console.log("Invalid file type", this)
            alert("Invalid file type")
          }
        }
      
      };
      console.log("water3")
      fileReader.readAsArrayBuffer(selectedFile);
}

// var base64String1 = "ttttt"
loadForm();
populatePairCoin();


// export {
//   ajaxCall,
//   backEndFileTypeValidation,
//   frontEndFileTypeValidation,
//   logoImgTypes,
//   logoImgHeight,
//   logoImgWidth,
//   isValidFile,
//   base64String1
// }







  <div style="display:flex; flex-direction: column; width:100%" id= "file-container">
  <div style="display:flex; flex-direction: row; width:100%">
    <input id="logo-file" class="logo-input-el" type="file"/>
    <button class="btn btn-primary" style ="display: none" id="removeImages">Remove Images</button>
  </div>
  <div  style="display: flex, flex-direction: column" id="preview-image"></div>
  </div>
        





Mar 11

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
  fileInputHandler2
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
        if (jsonEditorIds.includes(id)) {
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
    rows[0]["Th9YM"] = JSON.stringify(s3FileNameArray)
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
    
    // if((el["tabuuid"] = "3My8m") && (el["data"][5]["lable"] === "Th9YM"))
    // if(flag3){
    //   flag3 = false
    //   console.log("flag3", el["tabuuid"])
    //   let inputEl = document.createElement("input")
    //   inputEl.setAttribute("type", "file" )
    //   inputEl.setAttribute("id", "logo-file" )

    //   let inputAndButtonContainer = document.createElement("div")
    //   inputAndButtonContainer.setAttribute("style", "display:flex; flex-direction: column; width:100%")
    //   inputAndButtonContainer.appendChild(inputEl)
      

    //   let removeImgButton = document.createElement("button")
    //   removeImgButton.textContent = "Remove Image"
    //   removeImgButton.setAttribute(  "class" ,"btn btn-primary"  )
    //   removeImgButton.setAttribute("id", "removeImages" )
    //   removeImgButton.setAttribute("style" , "display: none" )
    //   inputAndButtonContainer.appendChild(removeImgButton)
    //   document.getElementById("file-container").appendChild(inputAndButtonContainer)

    //   let previewContainer = document.createElement("div")
    //   previewContainer.setAttribute("id", "preview-image")
    //   previewContainer.setAttribute("style", "display: flex, flex-direction: column" )
    //   document.getElementById("file-container").appendChild(previewContainer)
      
    // }

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

    // i is just a flag to handle behaviour of readAsDataURL() method
    // if(flag2){
    //   flag2 = false      
    //   // input image file is captured here 
    //   let fileInput = document.getElementById('logo-file');
    //   fileInput.onchange = function(){
    //     logoFlag = true 
    //     console.log("Onchange clicked", uploadedFileType.length)
    //     if(fileInput.value != ""){
    //       isValidFile = false
    //       flag = true
    //       var selectedFile = fileInput.files[0];
    //       let fileExtn = selectedFile.type
    //       console.log(selectedFile.type)
    //       console.log(selectedFile)
    //       var fileReader1 = new FileReader();
        
    //       if(uploadedFileType.length <2){
    //           fileReader1.onload =   function () {
    //             var base64String =   fileReader1.result.replace("data:", "")
    //                 .replace(/^.+,/, "");
    //             base64String1 =  base64String
    //             // console.log(base64String1);
    //             var fullB64Data = `data:${fileExtn};base64,${base64String1}`
    //             img = new Image();
    //             img.src = fullB64Data;
    //             img.onload = function() {
    //             // console.log(this.width, this.height,  "---------------------------")
    //             if(this.width <= logoImgWidth && this.height <= logoImgHeight){
    //             // if(true){
    //               let toHitBackEnd = true
    //               frontEndFileTypeValidation(selectedFile, img, toHitBackEnd)
    //               console.log(isValidFile, "img", img)  
    //             }
    //             else{
    //               alert(`Required Dimensions are Height x Width: ${logoImgHeight}px x ${logoImgWidth}px`);
    //             }
    //             }
    //           }
    //       }
    //       fileReader1.readAsDataURL(selectedFile);
    //     }
    //   }
    // }
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

// function backEndFileTypeValidation(fileReader, selectedFile ){
//   fileReader.onload =   function () {
//     var base64String =   fileReader.result.replace("data:", "")
//         .replace(/^.+,/, "");
//     console.log("=================",base64String)
//     let imageBase64Stringsep =  base64String;
//     base64String1 =  base64String
//     // console.log(base64String1);
//     uploadedFileNameArray.push(base64String1)
//     imgdata.base64String = uploadedFileNameArray
//   }
//   fileReader.readAsDataURL(selectedFile);
// }

// Front validation for determining the file type of the currently uploaded image file
// let flag = true
// function frontEndFileTypeValidation(selectedFile, img, toHitBackEnd){
//   // var blob = files[i];
//     // See step 1 above
    
//   var fileReader = new FileReader();
//   fileReader.onloadend = function(e) {
//     if(flag){
//       console.log(flag)
//       flag = false
//       let isValidFile = false;
//       console.log("water1.5")
//       var arr = (new Uint8Array(e.target.result)).subarray(0, 4);
//       // console.log(arr)
//       // console.log(e.target.result)
//       var header = "";
//       for(var i = 0; i < arr.length; i++) {
//         header += arr[i].toString(16);
//       }
//       console.log(header);
//       // Check the file signature against known types
//       let currentFileType
//       switch (header) {
//         case "89504e47":
//             isValidFile = true
//             console.log("png")
//             currentFileType = "png"
            
//             break;

//         case "3c3f786d":
//         case "3c737667":
//             isValidFile = true
//             console.log("svg")
//             currentFileType = "svg"
          
//             break;
//         default:
//             break;
//       }

//       // block of code to ensure only one svg file and only one png file is uploaded
//       if(isValidFile){
//           if((uploadedFileType.indexOf("png") !== -1) && (currentFileType === "png") && (uploadedFileType.length === 1)){
//                 alert("Upload a svg file")
//           }
//           else if((uploadedFileType.indexOf("svg") !== -1) && (currentFileType === "svg") && (uploadedFileType.length === 1)){
//                 alert("Upload a png file")
//           }
//           else{
//             uploadedFileType.push(currentFileType)
//             console.log(uploadedFileType)
//             let imageContainerEl = document.createElement("div")
//             imageContainerEl.setAttribute("style", "display:flex; align-items: center; justify-content: space-between")
//             imageContainerEl.appendChild(img)
//             document.getElementById("preview-image").appendChild(imageContainerEl)

//             //block of code to create a preview of the uploaded image along with the remove button
//             let deleteImageEl = document.createElement("button")
//             deleteImageEl.setAttribute("id", currentFileType)
//             deleteImageEl.textContent = "Delete Image"
//             deleteImageEl.setAttribute("class", "btn btn-primary")
//             imageContainerEl.appendChild(deleteImageEl)
//             deleteImageEl.setAttribute("style", "display: block; height: 40px;")
//             deleteImageEl.addEventListener("click", function(){
//               console.log("dfdsnfjdsnflj")
//               logoFlag = true
//               let tt = uploadedFileType.indexOf(deleteImageEl.getAttribute("id"))
//               uploadedFileType.splice(uploadedFileType.indexOf(deleteImageEl.getAttribute("id")), 1)
//               console.log(tt)
//               console.log(uploadedFileType)
//               document.getElementById("preview-image").removeChild(imageContainerEl)
//               imageContainerEl.removeChild(deleteImageEl)
//               document.getElementById('logo-file').value = ""

//             })
            
//             if(toHitBackEnd){
//               // console.log("hitBE")
//               backEndFileTypeValidation(fileReader, selectedFile)
//             }
//           }
//       }
//       else{
//         logoFlag = false
//         // console.log("Invalid file type", this)
//         alert("Invalid file type")
//       }
//     }
  
//   };
//   fileReader.readAsArrayBuffer(selectedFile);
// }

loadForm();
populatePairCoin();
fileInputHandler2(logoFlag,imgdata,uploadedFileNameArray, uploadedFileType, isValidFile, base64String1, img,  logoImgWidth, logoImgHeight);  
