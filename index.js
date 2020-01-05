const rp = require('request-promise')
const cheerio = require('cheerio');
const _ = require('lodash');

const AWS = require('aws-sdk');

AWS.config.update({
  region: "ap-northeast-1",
});

var docClient = new AWS.DynamoDB.DocumentClient();

AWS.config.update({region: 'us-west-2'});
const ses = new AWS.SES({apiVersion: '2010-12-01'});

const INTEREST_RATE_SPREAD = parseFloat(process.env.INTEREST_RATE_SPREAD) || 1.02;
const CS1 = parseFloat(process.env.CS1) || 100;
const C1 = parseFloat(process.env.C1) || 3;

// Create sendEmail params 
var params = {
    Destination: { /* required */
      CcAddresses: [
        /* more items */
      ],
      ToAddresses: [
        'luckstar77y@gmail.com',
        'cheners123@gmail.com',
        /* more items */
      ]
    },
    Message: { /* required */
      Body: { /* required */
        // Html: {
        //  Charset: "UTF-8",
        //  Data: "HTML_FORMAT_BODY"
        // },
        Text: {
         Charset: "UTF-8",
         Data: "TEXT_FORMAT_BODY"
        }
       },
       Subject: {
        Charset: 'UTF-8',
        Data: '3條均線糾結且外資投信連續買超3日'
       }
      },
    Source: 'luckstar77y@gmail.com', /* required */
    ReplyToAddresses: [
      /* more items */
    ],
  };

const worker = async (PickID, SubjectData) => {
  let dividends = await rp({
      uri: 'https://tw.screener.finance.yahoo.net/screener/ws',
      qs: {
        PickID,
        f:'j',
      },
      json: true
  });

  if(!parseInt(dividends.count)) return;

  let parseDividends = dividends.items.map(({
      symid: symbol,     //股票代號
      symname: twTitle,    //股名
      updn,        //漲跌(元)
      updn_rate,   //漲跌幅
      close_price: mC,         //當前價格
  }) => {
      let result = {
          symbol: cheerio.load(symbol).text(),
          twTitle: cheerio.load(twTitle).text(),
          updn,
          updn_rate,
          mC:  parseFloat(mC),
      }

      return result;
  })

  let $ = cheerio.load(await rp('https://www.taifex.com.tw/cht/2/stockLists'));
  const stockFutures = _.map($('#myTable tbody tr'), item => ({
      stockFutureSymbol: $(item).children('td').eq(0).text(),
      twTitleFull: $(item).children('td').eq(1).text(),
      symbol: $(item).children('td').eq(2).text(),
      twTitle: $(item).children('td').eq(3).text(),
      isStockFutureUnderlying: $(item).children('td').eq(4).text().trim() ? true : false,
      isStockOptionUnderlying: $(item).children('td').eq(5).text().trim() ? true : false,
      isStockExchangeUnderlying: $(item).children('td').eq(6).text().trim() ? true : false,
      isOTCUnderlying: $(item).children('td').eq(7).text().trim() ? true : false,
      isStockExchangeETFUnderlying: $(item).children('td').eq(8).text().trim() ? true : false,
      NumberOfStock: parseInt($(item).children('td').eq(9).text().replace(',','')),
  }));

  let notificationStocks = parseDividends.reduce((accu, curr) => {
      for(let stockFuture of stockFutures) {
          if(curr.symbol !== stockFuture.symbol) continue;
          if(curr.interestRateSpread < INTEREST_RATE_SPREAD) continue;
          if(curr.cs1 < CS1) continue;
          if(curr.c1 < C1) continue;

          accu.push({...curr, ...stockFuture});
          break;
      }
      return accu;
  }, []);

  if(_.isEmpty(notificationStocks)) {
    params.Message.Body.Text.Data = JSON.stringify({
      parseDividends,
    }, null, 2);
    params.Message.Subject.Data = SubjectData + '_沒有匹配';
    return await ses.sendEmail(params).promise();
  };
  parseDividends = notificationStocks;

  $ = cheerio.load(await rp({
      uri: 'https://goodinfo.tw/StockInfo/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FILTER_ITEM0=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S0=&FILTER_VAL_E0=&FILTER_ITEM1=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S1=&FILTER_VAL_E1=&FILTER_ITEM2=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S2=&FILTER_VAL_E2=&FILTER_ITEM3=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S3=&FILTER_VAL_E3=&FILTER_ITEM4=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S4=&FILTER_VAL_E4=&FILTER_ITEM5=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S5=&FILTER_VAL_E5=&FILTER_ITEM6=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S6=&FILTER_VAL_E6=&FILTER_ITEM7=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S7=&FILTER_VAL_E7=&FILTER_ITEM8=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S8=&FILTER_VAL_E8=&FILTER_ITEM9=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S9=&FILTER_VAL_E9=&FILTER_ITEM10=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S10=&FILTER_VAL_E10=&FILTER_ITEM11=---%E8%AB%8B%E9%81%B8%E6%93%87%E9%81%8E%E6%BF%BE%E6%A2%9D%E4%BB%B6---&FILTER_VAL_S11=&FILTER_VAL_E11=&FILTER_RULE0=%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%7C%7C%E6%88%90%E4%BA%A4%E5%83%B9%E5%9C%A85%E6%97%A5%E7%B7%9A%E4%B9%8B%E4%B8%8A%40%40%E6%88%90%E4%BA%A4%E5%83%B9%E5%9C%A8%E5%9D%87%E5%83%B9%E7%B7%9A%E4%B9%8B%E4%B8%8A%40%405%E6%97%A5%E7%B7%9A&FILTER_RULE1=%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%7C%7C%E6%88%90%E4%BA%A4%E5%83%B9%E5%9C%A810%E6%97%A5%E7%B7%9A%E4%B9%8B%E4%B8%8A%40%40%E6%88%90%E4%BA%A4%E5%83%B9%E5%9C%A8%E5%9D%87%E5%83%B9%E7%B7%9A%E4%B9%8B%E4%B8%8A%40%4010%E6%97%A5%E7%B7%9A&FILTER_RULE2=%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%7C%7C%E6%88%90%E4%BA%A4%E5%83%B9%E5%9C%A8%E6%9C%88%E7%B7%9A%E4%B9%8B%E4%B8%8A%40%40%E6%88%90%E4%BA%A4%E5%83%B9%E5%9C%A8%E5%9D%87%E5%83%B9%E7%B7%9A%E4%B9%8B%E4%B8%8A%40%40%E6%9C%88%E7%B7%9A&FILTER_RULE3=---%E8%AB%8B%E6%8C%87%E5%AE%9A%E9%81%B8%E8%82%A1%E6%A2%9D%E4%BB%B6---&FILTER_RULE4=%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%7C%7C5%E6%97%A5%2F10%E6%97%A5%2F%E6%9C%88%E7%B7%9A%E6%8E%A5%E8%BF%91%E6%88%96%E7%B3%BE%E7%B5%90%40%40%E5%9D%87%E5%83%B9%E7%B7%9A%E6%8E%A5%E8%BF%91%E6%88%96%E7%B3%BE%E7%B5%90%40%405%E6%97%A5%2F10%E6%97%A5%2F%E6%9C%88&FILTER_RULE5=---%E8%AB%8B%E6%8C%87%E5%AE%9A%E9%81%B8%E8%82%A1%E6%A2%9D%E4%BB%B6---&FILTER_RANK0=---%E8%AB%8B%E6%8C%87%E5%AE%9A%E6%8E%92%E5%90%8D%E6%A2%9D%E4%BB%B6---&FILTER_RANK1=---%E8%AB%8B%E6%8C%87%E5%AE%9A%E6%8E%92%E5%90%8D%E6%A2%9D%E4%BB%B6---&FILTER_RANK2=---%E8%AB%8B%E6%8C%87%E5%AE%9A%E6%8E%92%E5%90%8D%E6%A2%9D%E4%BB%B6---&FILTER_SHEET=%E5%B9%B4%E7%8D%B2%E5%88%A9%E8%83%BD%E5%8A%9B&FILTER_SHEET2=%E7%8D%B2%E5%88%A9%E8%83%BD%E5%8A%9B&FILTER_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83&FILTER_QUERY=%E6%9F%A5++%E8%A9%A2',
      headers: {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
      },
      json: true
  }));

  const stocksAboveMA = _.map($('#tblStockList tbody tr'), item => ({
      symbol: $(item).children('td').eq(0).text(),
      twTitle: $(item).children('td').eq(1).text(),
      mC: $(item).children('td').eq(2).text(),
  }));

  notificationStocks = parseDividends.reduce((accu, curr) => {
      for(let stockAboveMA of stocksAboveMA) {
          if(curr.symbol !== stockAboveMA.symbol) continue;

          accu.push({...curr, ...stockAboveMA});
          break;
      }
      return accu;
  }, []);
  
  if(_.isEmpty(notificationStocks)) {
    params.Message.Body.Text.Data = JSON.stringify({
      parseDividends,
      stocksAboveMA,
    }, null, 2);
    params.Message.Subject.Data = SubjectData + '_沒有匹配';
    return await ses.sendEmail(params).promise();
  };

  await new Promise((resolve, reject) => {
    const now = new Date();
    let types = [];
    PickID.includes('462') && types.push('外資');
    PickID.includes('480') && types.push('投信');
    docClient.batchWrite({
    RequestItems: {
      'stocker': notificationStocks.map(notificationStock => ({
        PutRequest: {
          Item:{
            ...notificationStock,
            created: now.getTime(),
            year: now.getFullYear(),
            month: now.getMonth() + 1,
            date: now.getDate(),
            types,
          }
        }
      })),
    }
  }, function(err, data) {
        if (err) {
            console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
            reject(err);
        } else {
            console.log("Added item:", JSON.stringify(data, null, 2));
            resolve(data);
        }
    });
  });

  params.Message.Body.Text.Data = JSON.stringify(notificationStocks, null, 2);
  params.Message.Subject.Data = SubjectData;
  return await ses.sendEmail(params).promise();
}

exports.handler = async function(event, context) {
  await worker('462', '3條均線糾結且外資連續買超3日');
  await worker('480', '3條均線糾結且投信連續買超3日');
  await worker('462,480', '3條均線糾結且外資投信連續買超3日');
  return 'ok';
}