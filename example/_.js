function teste() {
  //importanto a biblioteca
  eval(UrlFetchApp.fetch('https://raw.githubusercontent.com/moschionigabriel/tinydeskdata/master/tinydeskdata.js').getContentText())
  
  var seeds = [
    {name : 'raw_orders'
      ,source : {where : 'drive' ,config : {file_id : '16o1MH_ZxdcuPBLAnNjqTHbfbW40b3OTI'}}
      ,destination : {where : 'sql_platform',config : {platform : 'bigquery', credentials : {project_id : 'jaffle-shop-467514'},schema_name : 'seeds',table_name : 'raw_orders' , write_disposition : 'truncate'}}}
    
    ,{name : 'raw_customers'
      ,source : {where : 'drive' ,config : {file_id : '1yg_fRkooZXPcbDwpmxs1khR9IcsiEUjT'}}
      ,destination : {where : 'sql_platform',config : {platform : 'bigquery', credentials : {project_id : 'jaffle-shop-467514'},schema_name : 'seeds',table_name : 'raw_customers' , write_disposition : 'truncate'}}}
    
    ,{name : 'raw_payments'
        ,source : {where : 'drive' ,config : {file_id : '1n_weKGOQOvLCTbvUrr5zdEdy-BrBs9SO'}}
        ,destination : {where : 'sql_platform',config : {platform : 'bigquery', credentials : {project_id : 'jaffle-shop-467514'},schema_name : 'seeds',table_name : 'raw_payments' , write_disposition : 'truncate'}}}
  ];

  var jaffle_shop = {
    name : 'jaffle_shop'
    ,config : { platform : 'bigquery', credentials : {project_id : 'jaffle-shop-467514'},}
    ,models : [
      {name : 'stg_customers'
        ,schema_name : 'transform'
        ,materialized : 'view'
        ,description : 'testandi'
        ,columns : [{name: 'customer_id', description: 'testandi', tests : ['unique','not null']}]
        }
      
      ,{name : 'stg_orders'
        ,schema_name : 'transform'
        ,materialized : 'view'
        ,description : ''
        ,columns: [{name: 'order_id', description: '', tests : ['unique','not null']}
          ,{name: 'status', description: '', tests : [{accepted_values: {values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']}}]}]
        }
      
      ,{name : 'stg_payments'
        ,schema_name : 'transform'
        ,materialized : 'view'
        ,description : ''
        ,columns: [{name: 'payment_id', description: '', tests : ['unique','not null']}
          ,{name: 'payment_method', description: '', tests : [{accepted_values: {values: ['credit_card', 'coupon', 'bank_transfer', 'gift_card']}}]}]
        }
      ,{name: 'customers' 
        ,schema_name : 'transform'
        ,materialized : 'insert'
        ,description: 'This table has basic information about a customer, as well as some derived facts based on a customer"s orders'
        ,columns: [{name: 'customer_id', description: 'This is a unique identifier for a customer', tests : ['unique','not null']}
          ,{name: 'first_name', description: 'Customer"s first name. PII.'}
          ,{name: 'last_name', description: 'Customer"s last name. PII.'}
          ,{name: 'first_order', description: 'Date (UTC) of a customer"s first order'}
          ,{name: 'most_recent_order', description: 'Date (UTC) of a customer"s most recent order'}
          ,{name: 'number_of_orders', description: 'Count of the number of orders a customer has placed'}
          ,{name: 'total_order_amount', description: 'Total value (AUD) of a customer"s orders'}]
        }
      ,{name: 'orders' 
        ,schema_name : 'transform'
        ,materialized : 'insert'
        ,description: 'This table has basic information about orders, as well as some derived facts based on payments'
        ,columns: [
          {name: 'order_id', description: 'This is a unique identifier for an order', tests : ['unique','not null']}
          ,{name: 'customer_id', description: 'Foreign key to the customers table', tests : ['not null', {relationships: [{to:'ref("customers")'},{field:'customer_id'}]}]}
          ,{name: 'order_date', description: 'Date (UTC) that the order was placed'}
          ,{name: 'status', description: '{{ doc("orders_status") }}', tests : [{accepted_values: {values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']}}]}
          ,{name: 'amount', description: 'Total amount (AUD) of the order', tests : ['not null']}
          ,{name: 'credit_card_amount', description: 'Amount of the order (AUD) paid for by credit card', tests : ['not null']}
          ,{name: 'coupon_amount', description: 'Amount of the order (AUD) paid for by coupon', tests : ['not null']}
          ,{name: 'bank_transfer_amount', description: 'Amount of the order (AUD) paid for by bank transfer', tests : ['not null']}
          ,{name: 'gift_card_amount', description: 'Amount of the order (AUD) paid for by gift card', tests : ['not null']}
        ]
        }
      ]
  }


  var orchestra = {
    name : 'tinyDeskJaffleShop'
    ,log_destination : {folder_id : '11uG3nRE7EHulb-PDsQsPEH5nQv1l8FMr'}
    ,nodes : [
      {name : 'ingestion', info : seeds, depends_on : []}
      ,{name : 'modeling', info : jaffle_shop, depends_on : ['ingestion']}
    ]
  }

  //console.log(tinyDeskData)
  tinyDeskData.orchestrate(orchestra)
}